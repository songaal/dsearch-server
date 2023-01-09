package com.danawa.dsearch.server.indices.service;

import com.danawa.dsearch.server.indices.adapter.IndicesAdapter;
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
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class IndicesService {
    private static Logger logger = LoggerFactory.getLogger(IndicesService.class);
    private IndicesAdapter indicesEsAdapter;

    @Value("${dsearch.collection.index-suffix-a}")
    private String suffixA;
    @Value("${dsearch.collection.index-suffix-b}")
    private String suffixB;

    public IndicesService(IndicesAdapter indicesEsAdapter) {
        this.indicesEsAdapter = indicesEsAdapter;
    }

    public void createSystemIndex(UUID clusterId, String index, String source) throws IOException {
        indicesEsAdapter.createIndex(clusterId, index, source);
    }

    public DocumentPagination findAllDocumentPagination(UUID clusterId, String index, long pageNum, long rowSize, SearchSourceBuilder builder) throws IOException {
        return findAllDocumentPagination(clusterId, index, pageNum, rowSize, null, false, builder);
    }

    public DocumentPagination findAllDocumentPagination(UUID clusterId,
                                                        String index,
                                                        long pageNum,
                                                        long rowSize,
                                                        String id,
                                                        boolean analysis,
                                                        SearchSourceBuilder builder) throws IOException {
        SearchRequest searchRequest = makeSearchRequest(index, id, pageNum, rowSize, builder);
        SearchResponse searchResponse = indicesEsAdapter.getDocuments(clusterId, searchRequest);

        logger.info("{}", searchResponse);
        DocumentPagination documentPagination = new DocumentPagination();
        documentPagination.setTotalCount(searchResponse.getHits().getTotalHits().value);
        documentPagination.setHits(searchResponse.getHits().getHits());
        if (searchResponse.getAggregations() != null) {
            documentPagination.setAggregations(searchResponse.getAggregations().asMap());
        }
        documentPagination.setPageNum(pageNum);
        documentPagination.setRowSize(rowSize);

        Map<String, Object> fieldMap = getFields(clusterId, index);
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
                        analyzeTokens = indicesEsAdapter.analyze(clusterId, index, analyzer, String.valueOf(source.get(field)));
                    }
                    analyzerTextTerms.put(field, analyzeTokens);
                }
                analyzeDocumentTermMap.put(searchHits[i].getId(), analyzerTextTerms);
            }
        }
        documentPagination.setAnalyzeDocumentTermMap(analyzeDocumentTermMap);
        return documentPagination;
    }

    private SearchRequest makeSearchRequest(String index, String docId, long pageNum, long rowSize, SearchSourceBuilder builder){
        SearchRequest searchRequest = new SearchRequest();
        if (builder == null) {
            builder = new SearchSourceBuilder();

            if (docId != null && !"".equals(docId)) {
                builder
                        .query(QueryBuilders.boolQuery()
                                .must(new TermQueryBuilder("_id", docId)));
            } else {
                builder.query(QueryBuilders.matchAllQuery());
            }
        }

        searchRequest.indices(index).source(builder
                .from((int) ((int) pageNum * rowSize))
                .size((int) rowSize));
        return searchRequest;
    }

    public Map<String, Object> getFields(UUID clusterId, String index) throws IOException {
        return indicesEsAdapter.getFieldsMappings(clusterId, index);
    }


    public Map<String, List<DocumentAnalyzer.Analyzer>> getDocumentAnalyzer(UUID clusterId, String index, Map<String, List<DocumentAnalyzer.Analyzer>> documents) throws IOException {
        Map<String, List<DocumentAnalyzer.Analyzer>> analyzerMap = new HashMap<>();
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
                    List<AnalyzeResponse.AnalyzeToken> analyzeTokenList = indicesEsAdapter.analyze(clusterId, index, analyzer, text);
                    int tokenSize = analyzeTokenList.size();
                    for (int t = 0; t < tokenSize; t++) {
                        AnalyzeResponse.AnalyzeToken analyzeToken = analyzeTokenList.get(t);
                        termList.add(analyzeToken.getTerm());
                    }
                    fieldAnalyzer.setTerm(termList);
                } catch (Exception e) {
                    logger.error("", e.getMessage());
                }
            }
            analyzerMap.put(documentId, fields);
        }
        return analyzerMap;
    }

    public SearchResponse findAll(UUID clusterId, String index) throws IOException {
        return indicesEsAdapter.findAll(clusterId, index);
    }

    public void delete(UUID clusterId, String index) throws IOException {
        indicesEsAdapter.deleteIndex(clusterId, index);
    }

}
