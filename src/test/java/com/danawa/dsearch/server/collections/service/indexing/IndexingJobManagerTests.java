package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.collections.service.status.StatusService;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
public class IndexingJobManagerTests {
    private IndexingJobManager indexingJobManager;

    @Mock
    private IndexingJobService indexingJobService;

    @Mock
    private IndexerClient indexerClient;

    @Mock
    private StatusService statusService;

    @Mock
    private HistoryService historyService;

    private long timeout = 6480000;

    @BeforeEach
    public void setup(){
        this.indexingJobManager = new IndexingJobManager(indexingJobService, indexerClient, statusService, historyService, timeout);
    }

    @Test
    @DisplayName("색인 추가")
    public void add_indexing(){
        // To-Do
        // 내부 step 들에 따른 테스트 필요

        UUID clusterId = UUID.randomUUID();
        String collectionId = "collectionId";
        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setClusterId(clusterId);
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX); // 현재 색인 중 인걸로 추가

        Assertions.assertDoesNotThrow(() -> {
            doNothing().when(statusService).create(indexingStatus, "READY");
            indexingJobManager.add(collectionId, indexingStatus);

            Assertions.assertEquals(indexingStatus, indexingJobManager.getManageQueue(collectionId));

            indexingJobManager.add(collectionId, indexingStatus, false);
            Assertions.assertEquals(indexingStatus, indexingJobManager.getManageQueue(collectionId));

            indexingJobManager.add(collectionId, indexingStatus, true);
            Assertions.assertEquals(indexingStatus, indexingJobManager.getManageQueue(collectionId));
        });
    }

    @Test
    @DisplayName("두번 연속 색인 추가 시 에러 발생")
    public void add_indexing_when_twice(){
        UUID clusterId = UUID.randomUUID();
        String collectionId = "collectionId";
        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setClusterId(clusterId);
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX); // 현재 색인 중 인걸로 추가

        Assertions.assertThrows(IndexingJobFailureException.class, () -> {
            doNothing().when(statusService).create(indexingStatus, "READY");
            indexingJobManager.add(collectionId, indexingStatus);
            indexingJobManager.add(collectionId, indexingStatus);
        });
    }


    @Test
    @DisplayName("클러스터 아이디가 없이 색인 추가 시 에러 발생")
    public void add_indexing_when_cluster_id_is_null(){
        String collectionId = "collectionId";
        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX); // 현재 색인 중 인걸로 추가

        Assertions.assertThrows(IndexingJobFailureException.class, () -> {
            indexingJobManager.add(collectionId, indexingStatus);
        });
    }

    @Test
    @DisplayName("색인 단계 추가 없이 색인 추가 시 에러 발생")
    public void add_indexing_when_current_step_is_null(){
        String collectionId = "collectionId";
        UUID clusterId = UUID.randomUUID();
        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setClusterId(clusterId);

        Assertions.assertThrows(IndexingJobFailureException.class, () -> {
            indexingJobManager.add(collectionId, indexingStatus);
        });
    }
}
