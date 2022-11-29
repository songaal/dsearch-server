package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingActionType;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IndexingServiceTests {

    @Mock
    private IndexingJobManager indexingJobManager;
    @Mock
    private IndexingJobService indexingJobService;

    private IndexingService indexingService;

    @BeforeEach
    public void setup(){
        this.indexingService = new IndexingService(indexingJobService, indexingJobManager);
    }

    @Test
    @DisplayName("연속 색인 테스트")
    public void startIndexingAllTest() {
        // 정상 동작
        Assertions.assertDoesNotThrow(() -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");
            IndexingActionType actionType = IndexingActionType.ALL;
            String groupSeq = ""; // all indexing 일 경우엔 빈 스트링

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
            given(indexingJobService.indexingAndExpose(eq(clusterId), eq(collection))).willReturn(status);
            doNothing().when(indexingJobManager).add(collection.getId(), status);

            Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
            Assertions.assertEquals(response.get("result"), "success");
            Assertions.assertEquals(response.get("indexingStatus"), status);
        });
    }
    @Test
    @DisplayName("연속 색인 테스트 - Indexing 중 인 경우")
    public void startIndexingAll_but_is_running(){
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");
        IndexingActionType actionType = IndexingActionType.ALL;
        String groupSeq = ""; // all indexing 일 경우엔 빈 스트링

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        doNothing().when(indexingJobManager).setQueueStatus(collection.getId(), "ERROR");
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
        Assertions.assertEquals(response.get("result"), "fail");
    }

    @Test
    @DisplayName("전체 색인 테스트")
    public void startIndexingTest() {
        // 정상 동작
        Assertions.assertDoesNotThrow(() -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");
            IndexingActionType actionType = IndexingActionType.INDEXING;
            String groupSeq = "";

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
            given(indexingJobService.indexing(eq(clusterId), eq(collection))).willReturn(status);
            doNothing().when(indexingJobManager).add(collection.getId(), status);

            Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
            Assertions.assertEquals(response.get("result"), "success");
        });
    }

    @Test
    @DisplayName("전체 색인 테스트- 색인 중 인 경우")
    public void start_indexing_but_is_running() {
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");
        IndexingActionType actionType = IndexingActionType.INDEXING;
        String groupSeq = "";

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
        Assertions.assertEquals(response.get("result"), "fail");
    }


    @Test
    @DisplayName("교체 테스트 - 색인이 끝난 이후")
    public void start_expose_after_indexing() {
        // 정상 동작
        Assertions.assertDoesNotThrow(() -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");
            IndexingActionType actionType = IndexingActionType.EXPOSE;
            String groupSeq = "";

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
            Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
            Assertions.assertEquals(response.get("result"), "success");
        });
    }
    @Test
    @DisplayName("교체 테스트 - 색인 중 인 경우")
    public void start_expose_when_running_indexing() {
        // Indexing 중 인 경우
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");
        IndexingActionType actionType = IndexingActionType.EXPOSE;
        String groupSeq = "";

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
        Assertions.assertEquals(response.get("result"), "fail");
    }


    @Test
    @DisplayName("색인 중지 테스트")
    public void stopIndexingTest() {
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);
        status.setCurrentStep(IndexActionStep.FULL_INDEX);

        IndexingActionType actionType = IndexingActionType.STOP_INDEXING;
        String groupSeq = "";

        // 현재 인덱싱 진행 중
        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        doNothing().when(indexingJobService).stopIndexing(status);
        doNothing().when(indexingJobManager).setQueueStatus(collection.getId(), "STOP");
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
        Assertions.assertEquals(response.get("result"), "success");

        verify(indexingJobService, times(1)).stopIndexing(status);
        verify(indexingJobManager, times(1)).setQueueStatus(collection.getId(), "STOP");
        verify(indexingJobManager, times(2)).getManageQueue(collection.getId());
    }

    @Test
    @DisplayName("그룹 색인 테스트")
    public void startGroupIndexingTest() {
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");
        String groupSeq = "0";

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);
        status.setCurrentStep(IndexActionStep.FULL_INDEX);

        IndexingActionType actionType = IndexingActionType.SUB_START;

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        doNothing().when(indexingJobService).startGroupJob(status, collection, groupSeq);
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);

        verify(indexingJobService, times(1)).startGroupJob(status, collection, groupSeq);
        Assertions.assertEquals(response.get("result"), "success");

    }

    @Test
    @DisplayName("그룹 색인 테스트 - groupseq가 정상적이지 않을 경우")
    public void start_group_indexing_when_invalid_groupseq() {
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");
        String groupSeq = "0,2,3";

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);
        status.setCurrentStep(IndexActionStep.FULL_INDEX);

        IndexingActionType actionType = IndexingActionType.SUB_START;

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
        Assertions.assertEquals(response.get("result"), "fail");
    }
}
