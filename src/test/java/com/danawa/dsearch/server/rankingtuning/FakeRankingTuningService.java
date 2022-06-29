package com.danawa.dsearch.server.rankingtuning;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.rankingtuning.entity.RankingTuningRequest;
import com.danawa.dsearch.server.rankingtuning.service.RankingTuningService;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FakeRankingTuningService extends RankingTuningService {
    public FakeRankingTuningService(ElasticsearchFactory elasticsearchFactory) {
        super(elasticsearchFactory);
    }

    public Map<String, Object> getAnalyze(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException, ElasticQueryException {
        if(clusterId == null || rankingTuningRequest == null){
            throw new NullArgumentException("");
        }

        Map<String, Object> resultEntitiy = new HashMap<String, Object>();

        resultEntitiy.put("Total", "total");
        resultEntitiy.put("SearchResponse", "response");
        resultEntitiy.put("analyzerTokensMap", "tokens");

        return resultEntitiy;
    }
}
