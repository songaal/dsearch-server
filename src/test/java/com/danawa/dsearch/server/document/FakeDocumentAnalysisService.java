package com.danawa.dsearch.server.document;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.document.entity.RankingTuningRequest;
import com.danawa.dsearch.server.document.service.DocumentAnalysisService;
import org.apache.commons.lang.NullArgumentException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FakeDocumentAnalysisService extends DocumentAnalysisService {
    public FakeDocumentAnalysisService(ElasticsearchFactory elasticsearchFactory) {
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
