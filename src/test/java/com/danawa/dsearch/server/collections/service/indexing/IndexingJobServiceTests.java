package com.danawa.dsearch.server.collections.service.indexing;


import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.notice.AlertService;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class IndexingJobServiceTests {
    private IndexingJobService indexingJobService;

    private String jdbcSystemIndex = ".dsearch_jdbc";

    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    @Mock
    private HistoryService indexHistoryService;
    @Mock
    private AlertService alertService;
    @Mock
    private IndexerClient indexerClient;

    @BeforeEach
    public void setup(){
        this.indexingJobService = new IndexingJobService(
                elasticsearchFactory, jdbcSystemIndex,indexHistoryService, alertService, indexerClient);
    }

    @Test
    @DisplayName("색인 테스트")
    public void indexingTest(){
        // 정상 작동
//        Assertions.assertDoesNotThrow(() -> {
//            UUID clusterId = UUID.randomUUID();
//            Collection collection = new Collection();
//            boolean autoRun = true;
//            IndexActionStep step = IndexActionStep.FULL_INDEX;
//            String indexingJobId = "indexingJobId";
//
//            IndexingStatus indexingStatus = new IndexingStatus();
//            indexingStatus.setIndexingJobId(indexingJobId);
//
//            RestHighLevelClient restClient = mock(RestHighLevelClient.class);
//            GetResponse getResponse = mock(GetResponse.class);
//
//
//            given(elasticsearchFactory.getClient(clusterId)).willReturn(restClient);
//            given(restClient.get(any(), eq(RequestOptions.DEFAULT))).willReturn(getResponse);
//            given(indexerClient.startJob(any(Map.class), any(Collection.class))).willReturn(indexingJobId);
//
//            IndexingStatus status = indexingJobService.indexing(clusterId, collection, autoRun, step);
//
//
//        });


    }
}
