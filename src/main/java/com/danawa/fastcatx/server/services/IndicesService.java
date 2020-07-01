package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.DocumentPagination;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class IndicesService {
    private static Logger logger = LoggerFactory.getLogger(IndicesService.class);
    private final ElasticsearchFactory elasticsearchFactory;

    public IndicesService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void createSystemIndex(UUID clusterId, String index, String source) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        if (!client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
            client.indices().create(new CreateIndexRequest(index)
                            .source(StreamUtils.copyToString(new ClassPathResource(source).getInputStream(),
                                    Charset.defaultCharset()), XContentType.JSON),
                    RequestOptions.DEFAULT);
        }
        elasticsearchFactory.close(client);
    }

    public DocumentPagination findAllDocumentPagination(UUID clusterId, String index, long pageNum, long rowSize, SearchSourceBuilder builder) throws IOException {
        return findAllDocumentPagination(clusterId, index, pageNum, rowSize, null, false, builder);
    }

    public DocumentPagination findAllDocumentPagination(UUID clusterId, String index, long pageNum, long rowSize, String id, boolean analysis, SearchSourceBuilder builder) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        SearchRequest searchRequest = new SearchRequest();
        if (builder == null) {
            builder = new SearchSourceBuilder();

            if (id != null && !"".equals(id)) {
                builder
                        .query(QueryBuilders.boolQuery()
                                .must(new TermQueryBuilder("_id", id)));
            } else {
                builder.query(QueryBuilders.matchAllQuery());
            }
        }

        searchRequest.indices(index).source(builder
                .from((int) ((int) pageNum * rowSize))
                .size((int) rowSize));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        DocumentPagination documentPagination = new DocumentPagination();
        documentPagination.setTotalCount(searchResponse.getHits().getTotalHits().value);
        documentPagination.setHits(searchResponse.getHits().getHits());
        if (searchResponse.getAggregations() != null) {
            documentPagination.setAggregations(searchResponse.getAggregations().asMap());
        }
        documentPagination.setPageNum(pageNum);
        documentPagination.setRowSize(rowSize);

        Map<String, Object> fieldMap = getFields(client, index);
        documentPagination.setFields(fieldMap.keySet());

        long totalPageNum = documentPagination.getTotalCount() % documentPagination.getRowSize() == 0 ?
                documentPagination.getTotalCount() / documentPagination.getRowSize() :
                documentPagination.getTotalCount() / documentPagination.getRowSize() + 1;
        documentPagination.setLastPageNum(totalPageNum);

        // doc_id, <field, terms>
        Map<String, Map<String, List<AnalyzeResponse.AnalyzeToken>>> analyzeDocumentTermMap = new LinkedHashMap<>();

        if (analysis) {
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            int hitsSize = searchHits.length;
            for (int i = 0; i < hitsSize; i++) {
                Map<String, Object> source = searchHits[i].getSourceAsMap();

                Map<String, List<AnalyzeResponse.AnalyzeToken>> analyzerTextTerms = new HashMap<>();

                Iterator<String> iterator = fieldMap.keySet().iterator();
                while (iterator.hasNext()) {
                    String field = iterator.next();
                    Map<String, String> options = ((Map<String, String>) fieldMap.get(field));
                    String analyzer = options.get("analyzer") == null ? "standard" : options.get("analyzer");
                    List<AnalyzeResponse.AnalyzeToken> analyzeTokens = new ArrayList<>();
                    if (source.get(field) != null && !"".equals(source.get(field))) {
                        analyzeTokens = analyze(client, index, analyzer, String.valueOf(source.get(field)));
                    }
                    analyzerTextTerms.put(field, analyzeTokens);
                }
                analyzeDocumentTermMap.put(searchHits[i].getId(), analyzerTextTerms);
            }
        }
        documentPagination.setAnalyzeDocumentTermMap(analyzeDocumentTermMap);

        elasticsearchFactory.close(client);
        return documentPagination;
    }

    public Map<String, Object> getFields(RestHighLevelClient client, String index) throws IOException {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
        GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> mappings = response.mappings();
        Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
        Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
        return result;
    }

    public List<AnalyzeResponse.AnalyzeToken> analyze(RestHighLevelClient client, String index, String analyzer, String text) throws IOException {
        AnalyzeResponse response = client.indices()
                .analyze(AnalyzeRequest.withIndexAnalyzer(index, analyzer, text), RequestOptions.DEFAULT);
        List<AnalyzeResponse.AnalyzeToken> result = response.getTokens();
        return result;
    }

    public SearchResponse findAll(UUID clusterId, String index) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        Scroll scroll = new Scroll(new TimeValue(1000, TimeUnit.MILLISECONDS));
        SearchResponse response = client.search(new SearchRequest()
                        .indices(index)
                        .scroll(scroll)
                        .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder())),
                RequestOptions.DEFAULT);
        elasticsearchFactory.close(client);
        return response;
    }

}
