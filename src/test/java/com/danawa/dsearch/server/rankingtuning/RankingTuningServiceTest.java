package com.danawa.dsearch.server.rankingtuning;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.rankingtuning.entity.RankingTuningRequest;
import com.danawa.dsearch.server.rankingtuning.service.RankingTuningService;
import org.apache.commons.lang.NullArgumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class RankingTuningServiceTest {
    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    private RankingTuningService rankingTuningService;

    @BeforeEach
    public void setup(){
        this.rankingTuningService = new FakeRankingTuningService(elasticsearchFactory);
    }

    @Test
    @DisplayName("랭킹 튜닝 성공")
    public void get_analyze_success() throws IOException, ElasticQueryException {
        //given
        UUID clusterId = UUID.randomUUID();

        //when
        RankingTuningRequest request = new RankingTuningRequest();
        Map<String, Object> result = rankingTuningService.getAnalyze(clusterId, request);

        //then
        Assertions.assertFalse(result.isEmpty());
    }

    @Test
    @DisplayName("랭킹 튜닝 실패, clusterId 가 null 일 경우")
    public void get_analyze_fail_when_clusterId_is_null() throws IOException, ElasticQueryException {

        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            RankingTuningRequest request = new RankingTuningRequest();
            Map<String, Object> result = rankingTuningService.getAnalyze(null, request);
        });
    }

    @Test
    @DisplayName("랭킹 튜닝 실패, RankingTuningRequest 가 null 일 경우")
    public void get_analyze_fail_when_RankingTuningRequest_is_null() throws IOException, ElasticQueryException {

        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            RankingTuningRequest request = new RankingTuningRequest();
            Map<String, Object> result = rankingTuningService.getAnalyze(clusterId, null);
        });
    }

}
