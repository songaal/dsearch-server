package com.danawa.dsearch.server.rankingtuning.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;

import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.rankingtuning.entity.AnalyzerTokens;
import com.danawa.dsearch.server.rankingtuning.entity.RankingTuningRequest;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;


@Service
public class RankingTuningService {
    private static Logger logger = LoggerFactory.getLogger(RankingTuningService.class);
    private final ElasticsearchFactory elasticsearchFactory;

    public RankingTuningService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public Map<String, Object> getAnalyze(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException, ElasticQueryException {
        if(clusterId == null || rankingTuningRequest == null){
            throw new NullArgumentException("");
        }

        try {
            Map<String, Object> resultEntitiy = new HashMap<String, Object>();

            /* 여러개를 입력 했을 때 */
            if(rankingTuningRequest.isMultiple()){
                /* 1. index mapping 테이블 가져오기 */
                Map<String, Object> analyzers = new LinkedHashMap<String, Object>();

                Map<String, MappingMetadata> mappings = getMultipleMappings(clusterId, rankingTuningRequest);

                // key: index, value: property
                for(String _index : mappings.keySet()){
                    /* 인덱스 매핑에 있는 analyzer 가져오기 */
                    LinkedHashMap<String, Object> mapping = (LinkedHashMap) mappings.get(_index).getSourceAsMap().get("properties");
                    Map<String, Object> indexAnalyzer = new LinkedHashMap<String, Object>();
                    if(mapping != null){
                        for(String property: mapping.keySet()){
                            LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) mapping.get(property);
                            findAnalyzer(indexAnalyzer, property, linkedHashMap, "");
                        }
                    }
                    analyzers.put(_index, indexAnalyzer);
                }

                SearchResponse searchResponse = getMultipleSearch(clusterId, rankingTuningRequest);
                List<Map<String, Object>> searchHitsResponse = new ArrayList<>();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                Map<String, Map<String, List<AnalyzerTokens>>> analyzerTokensMap = new HashMap<>();
                for(int i = 0 ;i < searchHits.length; i++) {
                    String hitsIndex = searchHits[i].getIndex();
                    Map<String, List<AnalyzerTokens>> docWithTokensMap = new HashMap<>();
                    List<AnalyzerTokens> analyzerTokensList = new ArrayList<>();

                    Map<String, Object> source = searchHits[i].getSourceAsMap();
                    searchHitsResponse.add(source);
                    if(analyzers.get(hitsIndex) != null){
                        Map<String, Object> indexAnalyzer = (Map<String, Object>) analyzers.get(hitsIndex);

                        for(String field: source.keySet()){
                            if(indexAnalyzer.get(field) != null){
                                AnalyzeResponse analyzeResponse = getMultipleAnalyze(clusterId, hitsIndex, indexAnalyzer.get(field).toString(), source.get(field).toString());
                                AnalyzerTokens analyzerTokens = new AnalyzerTokens();
                                analyzerTokens.setField(field);
                                List<String> tokens = new ArrayList<>();
                                for (AnalyzeResponse.AnalyzeToken token : analyzeResponse.getTokens()) {
                                    tokens.add(token.getTerm());
                                }
                                analyzerTokens.setTokens(tokens);
                                analyzerTokensList.add(analyzerTokens);
                            }
                        }
                    }
                    source.put("_index", hitsIndex );
                    source.put("_id", searchHits[i].getId());
                    source.put("_explanation", searchHits[i].getExplanation());

                    docWithTokensMap.put(searchHits[i].getId(), analyzerTokensList);
                    if(analyzerTokensMap.get(hitsIndex) != null) {
                        analyzerTokensMap.get(hitsIndex).putAll(docWithTokensMap);
                    } else {
                        analyzerTokensMap.put(hitsIndex, docWithTokensMap);
                    }

                }

                resultEntitiy.put("Total", searchResponse.getHits().getTotalHits());
                resultEntitiy.put("SearchResponse", searchHitsResponse);
                resultEntitiy.put("analyzerTokensMap", analyzerTokensMap);

            }else{
                /* 1. index mapping 테이블 가져오기 */
                Map<String, Object> analyzers = new LinkedHashMap<String, Object>();
                Map<String, Object> result = getMappings(clusterId, rankingTuningRequest);

                for(String key : result.keySet()){
                    LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) result.get(key);
                    findAnalyzer(analyzers, key, linkedHashMap, "");
                }

                /* 2. Search 결과 가져오기 {indices}/_search?explain=true */
                SearchResponse searchResponse = getSearch(clusterId, rankingTuningRequest);

                /* 3. Search 결과로 가져온 Field중에 analyzer가 있다면 그 결과도 다시 재탐색 해서 가져오기 */
                SearchHit[] searchHits = searchResponse.getHits().getHits();

                // key: docId, value: field, list<token>
                Map<String, List<AnalyzerTokens>> analyzerTokensMap = new HashMap<>();
                List<Map<String, Object>> searchHitsResponse = new ArrayList<>();

                for(int i = 0 ;i < searchHits.length; i++){
                    Map<String, Object> source = searchHits[i].getSourceAsMap();
                    List<AnalyzerTokens> analyzerTokensList = new ArrayList<>();
                    searchHitsResponse.add(source);

                    for(String field: source.keySet()){
                        if(analyzers.get(field) != null){
                            logger.info("{}, {}", analyzers.get(field), source.get(field));
                            AnalyzeResponse analyzeResponse = getAnalyzeFromES(clusterId, rankingTuningRequest, (String) analyzers.get(field), (String) source.get(field));
                            AnalyzerTokens analyzerTokens = new AnalyzerTokens();
                            analyzerTokens.setField(field);
                            List<String> tokens = new ArrayList<>();
                            for (AnalyzeResponse.AnalyzeToken token : analyzeResponse.getTokens()) {
                                tokens.add(token.getTerm());
                            }
                            analyzerTokens.setTokens(tokens);
                            analyzerTokensList.add(analyzerTokens);
                        }
                    }

                    source.put("_id", searchHits[i].getId());
                    source.put("_explanation", searchHits[i].getExplanation());
                    analyzerTokensMap.put(searchHits[i].getId(), analyzerTokensList);
                }

                resultEntitiy.put("Total", searchResponse.getHits().getTotalHits());
                resultEntitiy.put("SearchResponse", searchHitsResponse);
                resultEntitiy.put("analyzerTokensMap", analyzerTokensMap);
            }

            return resultEntitiy;
        }catch (Exception e){
            throw new ElasticQueryException(e);
        }
    }

    private AnalyzeResponse getAnalyzeFromES(UUID clusterId, RankingTuningRequest rankingTuningRequest, String analyzer, String hitValue) throws IOException{
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(rankingTuningRequest.getIndex(), analyzer, hitValue);
            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
            return analyzeResponse;
        }
    }

    private void findAnalyzer(Map<String, Object> analyzers, String property, Map<String, Object> linkedHashMap, String prefix){
        for(String key : linkedHashMap.keySet()){
            if(key.equals("analyzer")){
                analyzers.put(property, linkedHashMap.get(key));
            }
        }
    }

    private SearchResponse getSearch(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(rankingTuningRequest.getIndex());

            String query = rankingTuningRequest.getText();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }

    private Map<String, Object> getMappings(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            String index = rankingTuningRequest.getIndex();
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> mappings = response.mappings();
            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
            return result;
        }
    }

    private Map<String, MappingMetadata> getMultipleMappings(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            String index = rankingTuningRequest.getIndex();
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> mappings = response.mappings();
//            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
//            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
            return mappings;
        }
    }

    private SearchResponse getMultipleSearch(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(rankingTuningRequest.getIndex());
            String query = rankingTuningRequest.getText();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }
    private AnalyzeResponse getMultipleAnalyze(UUID clusterId, String index, String analyzer, String hitValue) throws IOException{
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(index, analyzer, hitValue);
            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
            return analyzeResponse;
        }
    }
}
