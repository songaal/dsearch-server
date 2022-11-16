package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingActionType;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
    @DisplayName("연속색인 테스트")
    public void startIndexingAllTest() {
        // 정상 동작
        Assertions.assertDoesNotThrow(() -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");
            IndexingActionType type = IndexingActionType.ALL;

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
            given(indexingJobService.indexing(eq(clusterId), eq(collection), any(Queue.class))).willReturn(status);
            doNothing().when(indexingJobManager).add(collection.getId(), status);

            IndexingStatus result = indexingService.startIndexingAll(clusterId, collection);
            Assertions.assertEquals(result.getClusterId(), clusterId);
        });

        // Indexing 중 인 경우
        Assertions.assertThrows(IndexingJobFailureException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);

            indexingService.startIndexingAll(clusterId, collection);
        });

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
            IndexingActionType type = IndexingActionType.INDEXING;

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
            given(indexingJobService.indexing(eq(clusterId), eq(collection), any(Queue.class))).willReturn(status);
            doNothing().when(indexingJobManager).add(collection.getId(), status);

            IndexingStatus result = indexingService.startIndexing(clusterId, collection);
            Assertions.assertEquals(result.getClusterId(), clusterId);
        });

        // Indexing 중 인 경우
        Assertions.assertThrows(IndexingJobFailureException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);

            indexingService.startIndexing(clusterId, collection);
        });

    }

    @Test
    @DisplayName("교체 테스트")
    public void startExposeTest() {
        // 정상 동작
        Assertions.assertDoesNotThrow(() -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
            indexingService.startExpose(clusterId, collection);
            verify(indexingJobService, times(1)).expose(clusterId, collection);
        });

        // Indexing 중 인 경우
        Assertions.assertThrows(IndexingJobFailureException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            Collection collection = new Collection();
            collection.setId("CollectionId");
            collection.setName("name");
            collection.setBaseId("baseId");
            IndexingActionType type = IndexingActionType.INDEXING;

            IndexingStatus status = new IndexingStatus();
            status.setClusterId(clusterId);

            given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);

            indexingService.startExpose(clusterId, collection);
        });
    }

    @Test
    @DisplayName("색인 중지 테스트")
    public void stopIndexingTest() {
        // 정상 동작
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);
        status.setCurrentStep(IndexActionStep.FULL_INDEX);

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        doNothing().when(indexingJobService).stopIndexing(status);
        doNothing().when(indexingJobManager).setQueueStatus(collection.getId(), "STOP");
        indexingService.stopIndexing(collection);

        verify(indexingJobService, times(1)).stopIndexing(status);
        verify(indexingJobManager, times(1)).setQueueStatus(collection.getId(), "STOP");
        verify(indexingJobManager, times(2)).getManageQueue(collection.getId());

        // 색인이 중지되었는데 한번 더 중지 한 경우
        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(null);
        indexingService.stopIndexing(collection);
        verify(indexingJobManager, times(4)).getManageQueue(collection.getId());
    }

    @Test
    @DisplayName("그룹 색인 테스트")
    public void startGroupIndexingTest() {
        // 실제로 사용하는지 확인 필요


        // 정상 동작
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setId("CollectionId");
        collection.setName("name");
        collection.setBaseId("baseId");
        String groupSeq = "0";

        IndexingStatus status = new IndexingStatus();
        status.setClusterId(clusterId);
        status.setCurrentStep(IndexActionStep.FULL_INDEX);

        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        doNothing().when(indexingJobService).startGroupJob(status, collection, groupSeq);
        IndexingStatus result = indexingService.startGroupIndexing(collection, groupSeq);

        verify(indexingJobService, times(1)).startGroupJob(status, collection, groupSeq);
        Assertions.assertEquals(status, result);

        // 정상적이지 않은 groupSeq일 경우
        String invaildGroupSeq = "0,2,3";
        given(indexingJobManager.getManageQueue(collection.getId())).willReturn(status);
        result = indexingService.startGroupIndexing(collection, invaildGroupSeq);
        verify(indexingJobService, times(0)).startGroupJob(status, collection, invaildGroupSeq);
        Assertions.assertNotEquals(status, result);
    }
}
