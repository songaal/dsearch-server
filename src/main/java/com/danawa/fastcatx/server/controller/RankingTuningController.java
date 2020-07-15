package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.AnalyzerTokens;
import com.danawa.fastcatx.server.entity.RankingTuningRequest;
import com.danawa.fastcatx.server.services.RankingTuningService;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;


@RestController
@RequestMapping("/rankingtuning")
public class RankingTuningController {
    private static Logger logger = LoggerFactory.getLogger(RankingTuningController.class);


    @Autowired
    private RankingTuningService rankingTuningService;

    public RankingTuningController() {
    }

    public void findAnalyzer(Map<String, Object> analyzers, String property, Map<String, Object> linkedHashMap){
        for(String key : linkedHashMap.keySet()){
            if(key.equals("analyzer")){
                analyzers.put(property, linkedHashMap.get(key));
            }else{
                if(linkedHashMap.get(key) != null && linkedHashMap.get(key) instanceof LinkedHashMap){
                    Map<String, Object> map = (LinkedHashMap) linkedHashMap.get(key);
                    findAnalyzer(analyzers, property, map);
                }
            }
        }
    }
    @PostMapping("/")
    public ResponseEntity<?> getRankingTuning(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody RankingTuningRequest rankingTuningRequest) throws IOException{
        Map<String, Object> resultEntitiy = new HashMap<String, Object>();

        /* 여러개를 입력 했을 때 */
        if(rankingTuningRequest.getIsMultiple()){
            /* 1. index mapping 테이블 가져오기 */
            Map<String, Object> analyzers = new LinkedHashMap<String, Object>();
            Map<String, MappingMetaData> mappings = rankingTuningService.getMultipleMappings(clusterId, rankingTuningRequest);

            // key: index, value: property
            for(String _index : mappings.keySet()){
                /* 인덱스 매핑에 있는 analyzer 가져오기 */
                LinkedHashMap<String, Object> mapping = (LinkedHashMap) mappings.get(_index).getSourceAsMap().get("properties");
                Map<String, Object> indexAnalyzer = new LinkedHashMap<String, Object>();
                if(mapping != null){
                    for(String property: mapping.keySet()){
                        LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) mapping.get(property);
                        findAnalyzer(indexAnalyzer, property, linkedHashMap);
                    }
                }
                analyzers.put(_index, indexAnalyzer);
            }

            SearchResponse searchResponse = rankingTuningService.getMultipleSearch(clusterId, rankingTuningRequest);
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
                            AnalyzeResponse analyzeResponse = rankingTuningService.getMultipleAnalyze(clusterId, hitsIndex, (String) indexAnalyzer.get(field), (String) source.get(field));
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
                analyzerTokensMap.put(hitsIndex, docWithTokensMap);
            }

            resultEntitiy.put("Total", searchResponse.getHits().getTotalHits());
            resultEntitiy.put("SearchResponse", searchHitsResponse);
            resultEntitiy.put("analyzerTokensMap", analyzerTokensMap);

        }else{
            /* 1. index mapping 테이블 가져오기 */
            Map<String, Object> analyzers = new LinkedHashMap<String, Object>();
            Map<String, Object> result = rankingTuningService.getMappings(clusterId, rankingTuningRequest);

            for(String key : result.keySet()){
                LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) result.get(key);
                findAnalyzer(analyzers, key, linkedHashMap);
            }

            /* 2. Search 결과 가져오기 {indices}/_search?explain=true */
            SearchResponse searchResponse = rankingTuningService.getSearch(clusterId, rankingTuningRequest);

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
                        AnalyzeResponse analyzeResponse = rankingTuningService.getAnalyze(clusterId, rankingTuningRequest, (String) analyzers.get(field), (String) source.get(field));
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

        return new ResponseEntity<>(resultEntitiy, HttpStatus.OK);
    }
}
