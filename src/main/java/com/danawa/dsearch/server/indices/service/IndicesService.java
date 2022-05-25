package com.danawa.dsearch.server.indices.service;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.entity.DocumentAnalyzer;
import com.danawa.dsearch.server.indices.entity.DocumentPagination;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetadata;
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
import org.springframework.beans.factory.annotation.Value;
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

    private final String lastIndexStatusIndex = ".dsearch_last_index_status";
    private final String lastIndexStatusIndexJson = "last_index_status.json";
    private final String indexHistory = ".dsearch_index_history";
    private final String indexHistoryJson = "index_history.json";

    @Value("${dsearch.collection.index-suffix-a}")
    private String suffixA;
    @Value("${dsearch.collection.index-suffix-b}")
    private String suffixB;

    public IndicesService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        createSystemIndex(clusterId, lastIndexStatusIndex, lastIndexStatusIndexJson);
        createSystemIndex(clusterId, indexHistory, indexHistoryJson);
    }

    public void createSystemIndex(UUID clusterId, String index, String source) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            if (!client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                client.indices().create(new CreateIndexRequest(index)
                                .source(StreamUtils.copyToString(new ClassPathResource(source).getInputStream(),
                                        Charset.defaultCharset()), XContentType.JSON),
                        RequestOptions.DEFAULT);
            }
        }
    }

    public DocumentPagination findAllDocumentPagination(UUID clusterId, String index, long pageNum, long rowSize, SearchSourceBuilder builder) throws IOException {
        return findAllDocumentPagination(clusterId, index, pageNum, rowSize, null, false, builder);
    }

    public DocumentPagination findAllDocumentPagination(UUID clusterId, String index, long pageNum, long rowSize, String id, boolean analysis, SearchSourceBuilder builder) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
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

            logger.info("{}",searchResponse);
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
            return documentPagination;
        }
    }

    public Map<String, Object> getFields(RestHighLevelClient client, String index) throws IOException {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
        GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
        Map<String, MappingMetadata> mappings = response.mappings();
        Map<String, Object> properties = null;
        boolean isPass = false;
        if (!isPass) {
            try {
                properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
                isPass = true;
            } catch (Exception e) {
                isPass = false;
            }
        }
        if (!isPass) {
            try {
                properties = (Map<String, Object>) mappings.get(index + suffixA).getSourceAsMap().get("properties");
                isPass = true;
            } catch (Exception e) {
                isPass = false;
            }
        }
        if (!isPass) {
            try {
                properties = (Map<String, Object>) mappings.get(index + suffixB).getSourceAsMap().get("properties");
                isPass = true;
            } catch (Exception e) {
                isPass = false;
            }
        }

//        Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
        return isPass ? properties : new HashMap<>();
    }

    public List<AnalyzeResponse.AnalyzeToken> analyze(RestHighLevelClient client, String index, String analyzer, String text) throws IOException {
        AnalyzeResponse response = client.indices()
                .analyze(AnalyzeRequest.withIndexAnalyzer(index, analyzer, text), RequestOptions.DEFAULT);
        List<AnalyzeResponse.AnalyzeToken> result = response.getTokens();
        return result;
    }

    public Map<String, List<DocumentAnalyzer.Analyzer>> getDocumentAnalyzer(UUID clusterId, String index, Map<String, List<DocumentAnalyzer.Analyzer>> documents) throws IOException {
        Map<String, List<DocumentAnalyzer.Analyzer>> analyzerMap = new HashMap<>();
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Iterator<String> iterator = documents.keySet().iterator();
            while (iterator.hasNext()) {
                String documentId = iterator.next();
                List<DocumentAnalyzer.Analyzer> fields = documents.get(documentId);
                for (int i = 0; i < fields.size(); i++) {
                    try {
                        DocumentAnalyzer.Analyzer fieldAnalyzer = fields.get(i);
                        String text = fieldAnalyzer.getText();
                        String analyzer = fieldAnalyzer.getAnalyzer();
                        List<String> termList = new ArrayList<>();
                        List<AnalyzeResponse.AnalyzeToken> analyzeTokenList = analyze(client, index, analyzer, text);
                        int tokenSize = analyzeTokenList.size();
                        for (int t = 0; t< tokenSize; t++) {
                            AnalyzeResponse.AnalyzeToken analyzeToken = analyzeTokenList.get(t);
                            termList.add(analyzeToken.getTerm());
                        }
                        fieldAnalyzer.setTerm(termList);
                    } catch (Exception e) {
                        logger.warn("", e.getMessage());
                    }
                }
                analyzerMap.put(documentId, fields);
            }
        }
        return analyzerMap;
    }

    public SearchResponse findAll(UUID clusterId, String index) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Scroll scroll = new Scroll(new TimeValue(1000, TimeUnit.MILLISECONDS));
            SearchResponse response = client.search(new SearchRequest()
                            .indices(index)
                            .scroll(scroll)
                            .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder())),
                    RequestOptions.DEFAULT);
            return response;
        }
    }

    public GetResponse findById(UUID clusterId, String index, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            return client.get(new GetRequest().index(index).id(id), RequestOptions.DEFAULT);
        }
    }

    public void delete(UUID clusterId, String index) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
            deleteIndexRequest.indices(index);
            client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        }
    }

}
