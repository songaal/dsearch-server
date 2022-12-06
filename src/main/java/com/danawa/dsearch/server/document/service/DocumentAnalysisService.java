package com.danawa.dsearch.server.document.service;

import com.danawa.dsearch.server.document.dto.DocumentAnalysisReqeust;
import com.danawa.dsearch.server.document.entity.SearchQuery;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;


@Service
public class DocumentAnalysisService {
    private static Logger logger = LoggerFactory.getLogger(DocumentAnalysisService.class);

    private ElasticsearchFactoryHighLevelWrapper esFactoryHighLevelWrapper;
    public DocumentAnalysisService(
            ElasticsearchFactoryHighLevelWrapper esFactoryHighLevelWrapper) {
        this.esFactoryHighLevelWrapper = esFactoryHighLevelWrapper;
    }

    public Map<String, Object> analyzeDocument(UUID clusterId, DocumentAnalysisReqeust documentAnalysisReqeust){
        if(documentAnalysisReqeust.isViewDetails()){
            return analyzeDocumentViewDetails(clusterId, documentAnalysisReqeust);
        }else{
            return analyzeDocumentViewBrief(clusterId, documentAnalysisReqeust);
        }
    }

    private Map<String, Object> analyzeDocumentViewBrief(UUID clusterId, DocumentAnalysisReqeust documentAnalysisReqeust) {
        Map<String, Object> result = new HashMap<>();
        SearchQuery searchQuery = documentAnalysisReqeust.getSearchQuery();
        String index = searchQuery.getIndex();
        String query = searchQuery.getQuery();

        try {
            // 1. Index Mapping 가져오기
            Map<String, Object> mappings = esFactoryHighLevelWrapper.getIndexMappings(clusterId, index);

            // 2. 분석기가 셋팅된 필드만 파싱
            Map<String, Object> extractedMappings = extractAnalyzerFields(mappings);

            // 3. 만약 분석기가 셋팅되어 있지 않다면 종료
            if (extractedMappings.size() == 0) return result;

            // 4. 분석기가 셋팅되어 있다면 해당 필드만 검색
            String[] extractedFields = extractedMappings.keySet().toArray(new String[0]);

            // 5. 쿼리와 field 리스트를 합치기
            Map<String, Object> queryMap = JsonUtils.convertStringToMap(query);
            queryMap.put("_source", "[" + String.join(",", extractedFields) + "]");

            // 6. 문자열 형태로 변환 후 검색
            String mergedQuery = JsonUtils.convertMapToString(queryMap);
            List<Map<String, Object>> searchList = esFactoryHighLevelWrapper.search(clusterId, index, mergedQuery);

            // 7. termvector로 해당 필드의 내용 분석
            for(Map<String, Object> item: searchList){
                String docId= (String) item.get("_id");
                Map<String, Object> source = (Map<String, Object>) item.get("_source");
                Map<String, Object> termVectors = esFactoryHighLevelWrapper.getTermVectors(clusterId, index, docId, extractedFields);
                makePrettyFormat(result, termVectors, source);

                // 8. 포맷팅 작업
                // {
                //   "필드이름": {
                //     "document": "문자열",
                //     "documentTermVectors": [ "문자열1", "문자열2", ... ],
                //   }
                // }
//                for(String fieldName : termVectors.keySet()){
//                    Map<String, Object> data = new HashMap<>();
//                    data.put("document", source.get(fieldName));
//                    data.put("documentTermVectors", termVectors.get(fieldName));
//                    result.put(fieldName, data);
//                }
            }
        } catch (IOException e) {
            logger.error("", e);
        }

        // 9. 셋팅 후 리턴
        return result;
    }

    private Map<String, Object> extractAnalyzerFields(Map<String, Object> mappings){
        Map<String, Object> result = new HashMap<>();

        for (String field: mappings.keySet()){
            Map<String, Object> metadata = (Map<String, Object>) mappings.get(field);
            if(isAnalyzerSet(metadata)){
                result.put(field, metadata);
            }
        }

        return result;
    }

    private boolean isAnalyzerSet(Map<String, Object> metadata){
        if( metadata.get("analyzer") != null){
            return true;
        }else if (metadata.get("search_analyzer") != null){
            return true;
        }
        return false;
    }


    private Map<String, Object> analyzeDocumentViewDetails(UUID clusterId, DocumentAnalysisReqeust documentAnalysisReqeust){
        Map<String, Object> result = new HashMap<>();
        SearchQuery searchQuery = documentAnalysisReqeust.getSearchQuery();
        String index = searchQuery.getIndex();
        String query = searchQuery.getQuery();

        try {
            // 1. Index Mapping 가져오기
            Map<String, Object> mappings = esFactoryHighLevelWrapper.getIndexMappings(clusterId, index);

            // 2. 해당 필드만 검색
            String[] fields = mappings.keySet().toArray(new String[0]);

            // 3. 쿼리와 field 리스트를 합치기
            Map<String, Object> queryMap = JsonUtils.convertStringToMap(query);
            queryMap.put("_source", "[" + String.join(",", fields) + "]");

            // 4. 문자열 형태로 변환 후 검색
            String mergedQuery = JsonUtils.convertMapToString(queryMap);
            List<Map<String, Object>> searchList = esFactoryHighLevelWrapper.search(clusterId, index, mergedQuery);

            // 5. termvector로 해당 필드의 내용 분석
            for(Map<String, Object> item: searchList){
                String docId = (String) item.get("_id");
                Map<String, Object> source = (Map<String, Object>) item.get("_source");
                Map<String, Object> termVectors = esFactoryHighLevelWrapper.getTermVectors(clusterId, index, docId, fields);
                makePrettyFormat(result, source, termVectors);

                // 6. 포맷팅 작업
                // {
                //   "필드이름": {
                //     "document": "문자열",
                //     "documentTermVectors": [ "문자열1", "문자열2", ... ],
                //   }
                // }
//                for(String fieldName : source.keySet()){
//                    Map<String, Object> data = new HashMap<>();
//                    data.put("document", source.get(fieldName));
//                    data.put("documentTermVectors", termVectors.get(fieldName) == null ? "" : termVectors.get(fieldName));
//                    result.put(fieldName, data);
//                }
            }
        } catch (IOException e) {
            logger.error("", e);
        }

        // 9. 셋팅 후 리턴
        return result;
    }

    private void makePrettyFormat(Map<String, Object> result, Map<String, Object> originSource, Map<String, Object> joinSource){
        // 포맷팅 작업
        // {
        //   "필드이름": {
        //     "document": "문자열",
        //     "documentTermVectors": [ "문자열1", "문자열2", ... ],
        //   }
        // }
        for(String fieldName : originSource.keySet()){
            Map<String, Object> data = new HashMap<>();
            data.put("document", originSource.get(fieldName));
            data.put("documentTermVectors", joinSource.get(fieldName) == null ? "" : joinSource.get(fieldName));
            result.put(fieldName, data);
        }
    }


//    public Map<String, Object> getAnalyze(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException, ElasticQueryException {
//        if(clusterId == null || rankingTuningRequest == null){
//            throw new NullArgumentException("");
//        }
//
//        try {
//            Map<String, Object> resultEntitiy = new HashMap<String, Object>();
//
//            /* 여러개를 입력 했을 때 */
//            if(rankingTuningRequest.isMultiple()){
//                /* 1. index mapping 테이블 가져오기 */
//                Map<String, Object> analyzers = new LinkedHashMap<String, Object>();
//
//                Map<String, MappingMetadata> mappings = getMultipleMappings(clusterId, rankingTuningRequest);
//
//                // key: index, value: property
//                for(String _index : mappings.keySet()){
//                    /* 인덱스 매핑에 있는 analyzer 가져오기 */
//                    LinkedHashMap<String, Object> mapping = (LinkedHashMap) mappings.get(_index).getSourceAsMap().get("properties");
//                    Map<String, Object> indexAnalyzer = new LinkedHashMap<String, Object>();
//                    if(mapping != null){
//                        for(String property: mapping.keySet()){
//                            LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) mapping.get(property);
//                            findAnalyzer(indexAnalyzer, property, linkedHashMap, "");
//                        }
//                    }
//                    analyzers.put(_index, indexAnalyzer);
//                }
//
//                SearchResponse searchResponse = getMultipleSearch(clusterId, rankingTuningRequest);
//                List<Map<String, Object>> searchHitsResponse = new ArrayList<>();
//                SearchHit[] searchHits = searchResponse.getHits().getHits();
//                Map<String, Map<String, List<AnalyzerTokens>>> analyzerTokensMap = new HashMap<>();
//                for(int i = 0 ;i < searchHits.length; i++) {
//                    String hitsIndex = searchHits[i].getIndex();
//                    Map<String, List<AnalyzerTokens>> docWithTokensMap = new HashMap<>();
//                    List<AnalyzerTokens> analyzerTokensList = new ArrayList<>();
//
//                    Map<String, Object> source = searchHits[i].getSourceAsMap();
//                    searchHitsResponse.add(source);
//                    if(analyzers.get(hitsIndex) != null){
//                        Map<String, Object> indexAnalyzer = (Map<String, Object>) analyzers.get(hitsIndex);
//
//                        for(String field: source.keySet()){
//                            if(indexAnalyzer.get(field) != null){
//                                AnalyzeResponse analyzeResponse = getMultipleAnalyze(clusterId, hitsIndex, indexAnalyzer.get(field).toString(), source.get(field).toString());
//                                AnalyzerTokens analyzerTokens = new AnalyzerTokens();
//                                analyzerTokens.setField(field);
//                                List<String> tokens = new ArrayList<>();
//                                for (AnalyzeResponse.AnalyzeToken token : analyzeResponse.getTokens()) {
//                                    tokens.add(token.getTerm());
//                                }
//                                analyzerTokens.setTokens(tokens);
//                                analyzerTokensList.add(analyzerTokens);
//                            }
//                        }
//                    }
//                    source.put("_index", hitsIndex );
//                    source.put("_id", searchHits[i].getId());
//                    source.put("_explanation", searchHits[i].getExplanation());
//
//                    docWithTokensMap.put(searchHits[i].getId(), analyzerTokensList);
//                    if(analyzerTokensMap.get(hitsIndex) != null) {
//                        analyzerTokensMap.get(hitsIndex).putAll(docWithTokensMap);
//                    } else {
//                        analyzerTokensMap.put(hitsIndex, docWithTokensMap);
//                    }
//
//                }
//
//                resultEntitiy.put("Total", searchResponse.getHits().getTotalHits());
//                resultEntitiy.put("SearchResponse", searchHitsResponse);
//                resultEntitiy.put("analyzerTokensMap", analyzerTokensMap);
//
//            }else{
//                /* 1. index mapping 테이블 가져오기 */
//                Map<String, Object> analyzers = new LinkedHashMap<String, Object>();
//                Map<String, Object> result = getMappings(clusterId, rankingTuningRequest);
//
//                for(String key : result.keySet()){
//                    LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) result.get(key);
//                    findAnalyzer(analyzers, key, linkedHashMap, "");
//                }
//
//                /* 2. Search 결과 가져오기 {indices}/_search?explain=true */
//                SearchResponse searchResponse = getSearch(clusterId, rankingTuningRequest);
//
//                /* 3. Search 결과로 가져온 Field중에 analyzer가 있다면 그 결과도 다시 재탐색 해서 가져오기 */
//                SearchHit[] searchHits = searchResponse.getHits().getHits();
//
//                // key: docId, value: field, list<token>
//                Map<String, List<AnalyzerTokens>> analyzerTokensMap = new HashMap<>();
//                List<Map<String, Object>> searchHitsResponse = new ArrayList<>();
//
//                for(int i = 0 ;i < searchHits.length; i++){
//                    Map<String, Object> source = searchHits[i].getSourceAsMap();
//                    List<AnalyzerTokens> analyzerTokensList = new ArrayList<>();
//                    searchHitsResponse.add(source);
//
//                    for(String field: source.keySet()){
//                        if(analyzers.get(field) != null){
//                            logger.info("{}, {}", analyzers.get(field), source.get(field));
//                            AnalyzeResponse analyzeResponse = getAnalyzeFromES(clusterId, rankingTuningRequest, (String) analyzers.get(field), (String) source.get(field));
//                            AnalyzerTokens analyzerTokens = new AnalyzerTokens();
//                            analyzerTokens.setField(field);
//                            List<String> tokens = new ArrayList<>();
//                            for (AnalyzeResponse.AnalyzeToken token : analyzeResponse.getTokens()) {
//                                tokens.add(token.getTerm());
//                            }
//                            analyzerTokens.setTokens(tokens);
//                            analyzerTokensList.add(analyzerTokens);
//                        }
//                    }
//
//                    source.put("_id", searchHits[i].getId());
//                    source.put("_explanation", searchHits[i].getExplanation());
//                    analyzerTokensMap.put(searchHits[i].getId(), analyzerTokensList);
//                }
//
//                resultEntitiy.put("Total", searchResponse.getHits().getTotalHits());
//                resultEntitiy.put("SearchResponse", searchHitsResponse);
//                resultEntitiy.put("analyzerTokensMap", analyzerTokensMap);
//            }
//
//            return resultEntitiy;
//        }catch (Exception e){
//            throw new ElasticQueryException(e);
//        }
//    }

//    private AnalyzeResponse getAnalyzeFromES(UUID clusterId, RankingTuningRequest rankingTuningRequest, String analyzer, String hitValue) throws IOException{
//        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(rankingTuningRequest.getIndex(), analyzer, hitValue);
//            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
//            return analyzeResponse;
//        }
//    }
//
//    private void findAnalyzer(Map<String, Object> analyzers, String property, Map<String, Object> linkedHashMap, String prefix){
//        for(String key : linkedHashMap.keySet()){
//            if(key.equals("analyzer")){
//                analyzers.put(property, linkedHashMap.get(key));
//            }
//        }
//    }
//
//    private SearchResponse getSearch(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
//        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.indices(rankingTuningRequest.getIndex());
//
//            String query = rankingTuningRequest.getText();
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
//            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
//                searchSourceBuilder.parseXContent(parser);
//                searchRequest.source(searchSourceBuilder);
//            }
//            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//            return searchResponse;
//        }
//    }
//
//    private Map<String, Object> getMappings(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
//        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            String index = rankingTuningRequest.getIndex();
//            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
//            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
//            Map<String, MappingMetadata> mappings = response.mappings();
//            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
//            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
//            return result;
//        }
//    }
//
//    private Map<String, MappingMetadata> getMultipleMappings(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
//        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            String index = rankingTuningRequest.getIndex();
//            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
//            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
//            Map<String, MappingMetadata> mappings = response.mappings();
////            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
////            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
//            return mappings;
//        }
//    }
//
//    private SearchResponse getMultipleSearch(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
//        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.indices(rankingTuningRequest.getIndex());
//            String query = rankingTuningRequest.getText();
//            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
//            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
//                searchSourceBuilder.parseXContent(parser);
//                searchRequest.source(searchSourceBuilder);
//            }
//            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//            return searchResponse;
//        }
//    }
//    private AnalyzeResponse getMultipleAnalyze(UUID clusterId, String index, String analyzer, String hitValue) throws IOException{
//        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(index, analyzer, hitValue);
//            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
//            return analyzeResponse;
//        }
//    }
}
