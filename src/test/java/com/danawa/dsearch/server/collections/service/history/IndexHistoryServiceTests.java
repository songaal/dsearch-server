package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IndexHistoryServiceTests {
    private IndexHistoryService indexHistoryService;

    @Mock
    private IndexHistoryAdapter indexHistoryAdapter;
    @Mock
    private ElasticsearchFactory elasticsearchFactory;

    @BeforeEach
    public void setup(){
        this.indexHistoryService = new IndexHistoryService(this.indexHistoryAdapter, this.elasticsearchFactory);
    }

    @Test
    @DisplayName("초기화 테스트")
    public void initializeTest(){
        // DB로 바뀌면서 아무것도 진행하지 않음.
        Assertions.assertDoesNotThrow(() -> {
            this.indexHistoryService.initialize(UUID.randomUUID());
        });
    }

    @Test
    @DisplayName("컬렉션 히스토리 찾기")
    public void findByCollectionTest(){
        UUID clusterId = UUID.randomUUID();
        HistoryReadRequest historyReadRequest = new HistoryReadRequest();
        historyReadRequest.setIndexA("collection-a");
        historyReadRequest.setFrom(0);
        historyReadRequest.setSize(10);

        // 1. index만 있는 경우
        given(indexHistoryAdapter.findByCollection(clusterId, historyReadRequest)).willReturn(new ArrayList<>());
        List<Map<String, Object>> result = this.indexHistoryService.findByCollection(clusterId, historyReadRequest);
        Assertions.assertEquals(0, result.size());
        verify(indexHistoryAdapter, times(1)).findByCollection(clusterId, historyReadRequest);

        // 1.1 index만 있는 경우, 잘못된 index명인 경우는 그대로 검색
        // 컬렉션 인덱스로 생성 시 맨 뒤 두개 문자열이 추가 됨, 그러니 아래 처럼 검색을 해도 아마 나오지 않을 것.
        historyReadRequest.setIndexA("collection=aasdf");
        given(indexHistoryAdapter.findByCollection(clusterId, historyReadRequest)).willReturn(new ArrayList<>());
        result = this.indexHistoryService.findByCollection(clusterId, historyReadRequest);
        Assertions.assertEquals(0, result.size());
        verify(indexHistoryAdapter, times(2)).findByCollection(clusterId, historyReadRequest);

        // 2. index가 있고 jobType이 있는 경우
        historyReadRequest.setJobType("FULL_INDEX");
        given(indexHistoryAdapter.findByCollectionWithJobType(clusterId, historyReadRequest)).willReturn(new ArrayList<>());
        result = this.indexHistoryService.findByCollection(clusterId, historyReadRequest);
        Assertions.assertEquals(0, result.size());
        verify(indexHistoryAdapter, times(1)).findByCollectionWithJobType(clusterId, historyReadRequest);


        // 3. indexA, indexB가 없고 jobType만 있을 경우
        historyReadRequest.setIndexA(null);
        historyReadRequest.setIndexB(null);
        historyReadRequest.setJobType("FULL_INDEX");
        given(indexHistoryAdapter.findByClusterIdAndJobType(clusterId, historyReadRequest)).willReturn(new ArrayList<>());
        result = this.indexHistoryService.findByCollection(clusterId, historyReadRequest);
        Assertions.assertEquals(0, result.size());
        verify(indexHistoryAdapter,  times(1)).findByClusterIdAndJobType(clusterId, historyReadRequest);

    }


    @Test
    @DisplayName("컬렉션 히스토리 갯수 구하기")
    public void getTotalSizeTest(){
        UUID clusterId = UUID.randomUUID();
        HistoryReadRequest historyReadRequest = new HistoryReadRequest();
        historyReadRequest.setFrom(0);
        historyReadRequest.setSize(10);

        // 1. 인덱스가 없고 clusterId만 있는 경우
        given(indexHistoryAdapter.countByClusterId(clusterId)).willReturn(10L);
        long result = this.indexHistoryService.getTotalSize(clusterId, historyReadRequest);
        Assertions.assertEquals(10L, result);
        verify(indexHistoryAdapter, times(1)).countByClusterId(clusterId);

        // 2. 인덱스가 있는 경우
        historyReadRequest.setIndexA("indexA-a");
        given(indexHistoryAdapter.countByClusterIdAndIndex(clusterId, historyReadRequest)).willReturn(0L);
        result = this.indexHistoryService.getTotalSize(clusterId, historyReadRequest);
        Assertions.assertEquals(0L, result);
        verify(indexHistoryAdapter, times(1)).countByClusterIdAndIndex(clusterId, historyReadRequest);

    }

    @Test
    @DisplayName("ES 조회 후 히스토리 생성 하기")
    public void createHistoryByIndexingStatusTest(){
        /**
         * 특이 사항: 30초 대기 후 실행
         */
        UUID clusterId = UUID.randomUUID();
        String status = "SUCCESS";
        Map< String, Object > defaultMap = new HashMap<>();
        defaultMap.put("docs.count", "0");
        defaultMap.put("store.size", "0");

        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX);
        indexingStatus.setClusterId(clusterId);
        indexingStatus.setIndex("index");

        doNothing().when(indexHistoryAdapter).create(any(IndexHistory.class));
        given(elasticsearchFactory.catIndex(clusterId, "index")).willReturn(defaultMap); // elasticsearch 관련 메서드 모음
        this.indexHistoryService.create(indexingStatus, status);
        verify(indexHistoryAdapter, times(1)).create(any(IndexHistory.class));
    }

    @Test
    @DisplayName("파라미터로 고정된 갯수의 히스토리 생성 하기")
    public void createHistoryByParameterTest(){
        /**
         * 특이 사항: 30초 대기 후 실행
         */
        UUID clusterId = UUID.randomUUID();
        String status = "SUCCESS";

        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX);
        indexingStatus.setClusterId(clusterId);
        indexingStatus.setIndex("index");

        // 1. 인덱스가 없고 clusterId만 있는 경우
        doNothing().when(indexHistoryAdapter).create(any(IndexHistory.class));
        this.indexHistoryService.create(indexingStatus, "0", status, "0");
        verify(indexHistoryAdapter, times(1)).create(any(IndexHistory.class));
    }

    @Test
    @DisplayName("인덱스 히스토리 삭제")
    public void deleteIndexHistoryTest(){
        UUID clusterId = UUID.randomUUID();
        String collectionId = "collectionId";

        doNothing().when(indexHistoryAdapter).deleteAll(clusterId, collectionId);
        this.indexHistoryService.delete(clusterId, collectionId);
        verify(indexHistoryAdapter, times(1)).deleteAll(clusterId, collectionId);
    }
}
