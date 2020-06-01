package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class IndicesService {
    private static Logger logger = LoggerFactory.getLogger(IndicesService.class);
    private RestHighLevelClient client;

    public IndicesService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient) {
        this.client = restHighLevelClient;
    }

    public DocumentPagination findAllDocumentPagination(String index, long pageNum, long rowSize, String id) throws IOException {
        return findAllDocumentPagination(index, pageNum, rowSize, id, null);
    }
    public DocumentPagination findAllDocumentPagination(String index, long pageNum, long rowSize, SearchSourceBuilder builder) throws IOException {
        return findAllDocumentPagination(index, pageNum, rowSize, null, builder);
    }
    public DocumentPagination findAllDocumentPagination(String index, long pageNum, long rowSize, String id, SearchSourceBuilder builder) throws IOException {
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
        builder.from((int) ((int) pageNum * rowSize)).size((int) rowSize);

        searchRequest.indices(index).source(builder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        DocumentPagination documentPagination = new DocumentPagination();
//        기본값: 최대 1만건.
//        documentPagination.setTotalCount(getDocumentCount(index));
        documentPagination.setTotalCount(searchResponse.getHits().getTotalHits().value);
        documentPagination.setHits(searchResponse.getHits().getHits());
        documentPagination.setPageNum(pageNum);
        documentPagination.setRowSize(rowSize);

        Map<String, Object> fieldMap = getFields(index);
        documentPagination.setFields(fieldMap.keySet());

        long totalPageNum = documentPagination.getTotalCount() % documentPagination.getRowSize() == 0 ?
                documentPagination.getTotalCount() / documentPagination.getRowSize() :
                documentPagination.getTotalCount() / documentPagination.getRowSize() + 1;
        documentPagination.setLastPageNum(totalPageNum);


        // doc_id, <field, terms>
        Map<String, Map<String, List<AnalyzeResponse.AnalyzeToken>>> analyzeDocumentTermMap = new LinkedHashMap<>();
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
                List<AnalyzeResponse.AnalyzeToken> analyzeTokens = analyze(index, analyzer, String.valueOf(source.get(field)));
                analyzerTextTerms.put(field, analyzeTokens);
            }
            analyzeDocumentTermMap.put(searchHits[i].getId(), analyzerTextTerms);
        }
        documentPagination.setAnalyzeDocumentTermMap(analyzeDocumentTermMap);

        return documentPagination;
    }

    public long getDocumentCount(String indices) throws IOException {
        return client.count(new CountRequest().indices(indices), RequestOptions.DEFAULT).getCount();
    }

    public Map<String, Object> getFields(String index) throws IOException {
        GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
        GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
        Map<String, MappingMetaData> mappings = response.mappings();
        Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
        return mappings.get(index).getSourceAsMap().get("properties") == null ?
                new HashMap<>() : properties;
    }

    public List<AnalyzeResponse.AnalyzeToken> analyze(String index, String analyzer, String text) throws IOException {
        AnalyzeResponse response = client.indices()
                .analyze(AnalyzeRequest.withIndexAnalyzer(index, analyzer, text), RequestOptions.DEFAULT);
        return response.getTokens();
    }

}
