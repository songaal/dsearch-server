package com.danawa.dsearch.server.collections.service.status;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexingStep;
import com.danawa.dsearch.server.collections.service.status.entity.IndexStatus;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IndexStatusServiceTests {
    private IndexStatusService indexStatusService;

    @Mock
    private IndexStatusAdapter indexStatusAdapter;

    @BeforeEach
    public void setup(){
        this.indexStatusService = new IndexStatusService(indexStatusAdapter);
    }

    @Test
    @DisplayName("초기화 테스트")
    public void initializeTest(){
        // DB로 바뀌면서 아무것도 진행하지 않음.
        Assertions.assertDoesNotThrow(() -> {
            this.indexStatusService.initialize(UUID.randomUUID());
        });
    }

    @Test
    @DisplayName("상태 생성 테스트")
    public void createTest(){
        UUID clusterId = UUID.randomUUID();
        String currentStatus = "SUCCESS";

        IndexingInfo indexingInfo = new IndexingInfo();
        Collection collection = new Collection();
        collection.setId("collectionId");
        indexingInfo.setCollection(collection);
        indexingInfo.setCurrentStep(IndexingStep.FULL_INDEX);
        indexingInfo.setClusterId(clusterId);
        indexingInfo.setIndex("index");

        doNothing().when(this.indexStatusAdapter).create(any(IndexStatus.class));
        this.indexStatusService.create(indexingInfo, currentStatus);
        verify(this.indexStatusAdapter, times(1)).create(any(IndexStatus.class));
    }

    @Test
    @DisplayName("상태 삭제 테스트")
    public void deleteTest(){
        UUID clusterId = UUID.randomUUID();
        String index = "index";
        long startTime = 0;

        doNothing().when(this.indexStatusAdapter).delete(clusterId, index, startTime);
        this.indexStatusService.delete(clusterId, index, startTime);
        verify(this.indexStatusAdapter, times(1)).delete(clusterId, index, startTime);
    }

    @Test
    @DisplayName("상태 수정 테스트")
    public void updateTest(){
        UUID clusterId = UUID.randomUUID();
        String index = "index";
        long startTime = 0;
        String status = "SUCCESS";

        IndexingInfo indexingInfo = new IndexingInfo();
        Collection collection = new Collection();
        collection.setId("collectionId");
        indexingInfo.setCollection(collection);
        indexingInfo.setCurrentStep(IndexingStep.FULL_INDEX);
        indexingInfo.setClusterId(clusterId);
        indexingInfo.setIndex(index);
        indexingInfo.setStartTime(startTime);

        doNothing().when(this.indexStatusAdapter).create(any(IndexStatus.class));
        doNothing().when(this.indexStatusAdapter).delete(clusterId, index, startTime);
        this.indexStatusService.update(indexingInfo, status);

        verify(this.indexStatusAdapter, times(1)).delete(clusterId, index, startTime);
        verify(this.indexStatusAdapter, times(1)).create(any(IndexStatus.class));
    }

    @Test
    @DisplayName("상태 찾기 테스트")
    public void findAllTest(){
        UUID clusterId = UUID.randomUUID();
        int size = 10;
        int from = 0;

        given(this.indexStatusAdapter.findAll(clusterId, size, from)).willReturn(new ArrayList<>());
        List<Map<String, Object>> result = this.indexStatusService.findAll(clusterId, size, from);

        verify(this.indexStatusAdapter, times(1)).findAll(clusterId, size, from);
        Assertions.assertEquals(0, result.size());
    }
}
