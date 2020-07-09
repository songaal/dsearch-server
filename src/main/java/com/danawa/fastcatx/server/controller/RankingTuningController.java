package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.AnalyzerTokens;
import com.danawa.fastcatx.server.entity.ClusterStatusRequest;
import com.danawa.fastcatx.server.entity.ClusterStatusResponse;
import com.danawa.fastcatx.server.entity.RankingTuningRequest;
import com.danawa.fastcatx.server.services.ClusterService;
import com.danawa.fastcatx.server.services.ElasticsearchProxyService;
import com.danawa.fastcatx.server.services.RankingTuningService;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;


@RestController
@RequestMapping("/rankingtuning")
public class RankingTuningController {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);


    @Autowired
    private RankingTuningService rankingTuningService;

    public RankingTuningController() {
    }

    public void findAnalyzer(Map<String, String> analyzers, String key, Map<String, Object> linkedHashMap){
        for(String key2 : linkedHashMap.keySet()){
            if(key2.equals("analyzer")){
                analyzers.put(key, (String) linkedHashMap.get(key2));
            }else{
                if(linkedHashMap.get(key2) != null && linkedHashMap.get(key2) instanceof LinkedHashMap){
                    Map<String, Object> lMap = (LinkedHashMap) linkedHashMap.get(key2);
                    findAnalyzer(analyzers, key, lMap);
                }

            }
        }
    }
    @PostMapping("/")
    public ResponseEntity<?> getRankingTuning(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody RankingTuningRequest rankingTuningRequest) throws IOException{
        Map<String, Object> resultEntitiy = new HashMap<String, Object>();

        /* 1. index mapping 테이블 가져오기 */
        Map<String, String> analyzers = new LinkedHashMap<String, String>();
        Map<String, Object> result = rankingTuningService.getMappings(clusterId, rankingTuningRequest);
        for(String key : result.keySet()){
            LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) result.get(key);
            findAnalyzer(analyzers, key, linkedHashMap);
        }
        /* 2. Search 결과 가져오기 prod_test4/_search?explain=true */
        SearchResponse searchResponse = rankingTuningService.getSearch(clusterId, rankingTuningRequest);

        /* 3. Search 결과로 가져온 Field중에 analyzer가 있다면 그 결과도 다시 재 탐색 해서 가져오기 */
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
                    AnalyzeResponse analyzeResponse = rankingTuningService.getAnalyze(clusterId, rankingTuningRequest, analyzers.get(field), (String) source.get(field));
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
        return new ResponseEntity<>(resultEntitiy, HttpStatus.OK);
    }
}
