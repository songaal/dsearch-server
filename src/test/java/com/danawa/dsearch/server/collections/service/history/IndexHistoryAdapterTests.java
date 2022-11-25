package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.service.history.repository.HistoryRepository;
import com.danawa.dsearch.server.collections.service.history.specification.IndexHistorySpecification;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;

import java.util.*;

import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
public class IndexHistoryAdapterTests {

    @Mock
    private HistoryRepository historyRepository;

    private IndexHistoryAdapter indexHistoryAdapter;

    @BeforeEach
    public void setup(){
        indexHistoryAdapter = new IndexHistoryAdapter(historyRepository);
    }

    @Test
    @DisplayName("컬렉션 히스토리 생성 테스트")
    public void createTests(){
        UUID clusterId = UUID.randomUUID();
        IndexHistory createdHistory = new IndexHistory();
        createdHistory.setIndex("index");
        createdHistory.setId(1L);
        createdHistory.setClusterId(clusterId);

        IndexHistory indexHistory = new IndexHistory();
        indexHistory.setIndex("index");
        createdHistory.setClusterId(clusterId);

        when(historyRepository.save(indexHistory)).thenReturn(createdHistory);
        indexHistoryAdapter.create(indexHistory);
        verify(historyRepository, times(1)).save(indexHistory);
    }


    @Test
    @DisplayName("컬렉션 히스토리 전체 삭제 테스트")
    public void deleteAll(){
        // TO Do
        UUID clusterId = UUID.randomUUID();
        String collectionName = "collectionName";
        List<IndexHistory> emptyHistory = new ArrayList<>();
        when(historyRepository.findAll(any(Specification.class))).thenReturn(emptyHistory);
        doNothing().when(historyRepository).deleteAll(emptyHistory);
        indexHistoryAdapter.deleteAll(clusterId, collectionName);
        verify(historyRepository, times(1)).findAll(any(Specification.class));
        verify(historyRepository, times(1)).deleteAll(emptyHistory);
    }

    @Test
    @DisplayName("컬렉션 히스토리 카운트 테스트 - 특정 클러스터 및 Request로만")
    public void countByClusterIdAndIndexTest(){
        UUID clusterId = UUID.randomUUID();
        String collectionName = "collectionName";
        HistoryReadRequest historyReadRequest = new HistoryReadRequest();
        historyReadRequest.setIndexA(collectionName + "-a");
        historyReadRequest.setIndexB(collectionName + "-b");

        when(historyRepository.count(any(Specification.class))).thenReturn(0L);
        long count = indexHistoryAdapter.countByClusterIdAndIndex(clusterId, historyReadRequest);
        verify(historyRepository, times(1)).count(any(Specification.class));
        Assertions.assertEquals(0, count);
    }

    @Test
    @DisplayName("컬렉션 히스토리 카운트 테스트 - 특정 클러스터")
    public void countByClusterIdTest(){
        UUID clusterId = UUID.randomUUID();

        when(historyRepository.countByClusterId(clusterId)).thenReturn(0L);
        long count = indexHistoryAdapter.countByClusterId(clusterId);
        verify(historyRepository, times(1)).countByClusterId(clusterId);
        Assertions.assertEquals(0, count);
    }

    @Test
    @DisplayName("컬렉션 히스토리 컬렉션 명으로 찾기")
    public void findByCollectionTest(){
        UUID clusterId = UUID.randomUUID();
        String collectionName = "collectionName";
        int size = 10;
        int from = 0;

        HistoryReadRequest historyReadRequest = new HistoryReadRequest();
        historyReadRequest.setIndexA(collectionName + "-a");
        historyReadRequest.setIndexB(collectionName + "-b");
        historyReadRequest.setSize(size);
        historyReadRequest.setFrom(from);

        List<IndexHistory> out = new ArrayList<>();
        out.add(new IndexHistory());

        when(historyRepository.findAll(any(Specification.class), any(Pageable.class))).thenReturn(Page.empty());
        List<Map<String, Object>> result = indexHistoryAdapter.findByCollection(clusterId, historyReadRequest);
        verify(historyRepository, times(1)).findAll(any(Specification.class), any(Pageable.class));
        Assertions.assertEquals(0, result.size());
    }

    @Test
    @DisplayName("컬렉션 히스토리 - 클러스터Id와 색인 타입으로 찾기")
    public void findByClusterIdAndJobTypeTest(){
        UUID clusterId = UUID.randomUUID();
        String collectionName = "collectionName";
        String jobType = "FULL_INDEX";
        int size = 10;
        int from = 0;

        HistoryReadRequest historyReadRequest = new HistoryReadRequest();
        historyReadRequest.setIndexA(collectionName + "-a");
        historyReadRequest.setIndexB(collectionName + "-b");
        historyReadRequest.setJobType(jobType);
        historyReadRequest.setSize(size);
        historyReadRequest.setFrom(from);

        List<IndexHistory> out = new ArrayList<>();
        out.add(new IndexHistory());

        when(historyRepository.findAll(any(Specification.class), any(Pageable.class))).thenReturn(Page.empty());
        List<Map<String, Object>> result = indexHistoryAdapter.findByClusterIdAndJobType(clusterId, historyReadRequest);
        verify(historyRepository, times(1)).findAll(any(Specification.class), any(Pageable.class));
        Assertions.assertEquals(0, result.size());
    }

    @Test
    @DisplayName("인덱스 히스토리 클러스터Id와 컬렉션명 그리고 업무 타입으로 찾기")
    public void findByCollectionWithJobTypeTest(){
        UUID clusterId = UUID.randomUUID();
        String collectionName = "collectionName";
        String jobType = "FULL_INDEX";
        int size = 10;
        int from = 0;

        HistoryReadRequest historyReadRequest = new HistoryReadRequest();
        historyReadRequest.setIndexA(collectionName + "-a");
        historyReadRequest.setIndexB(collectionName + "-b");
        historyReadRequest.setJobType(jobType);
        historyReadRequest.setSize(size);
        historyReadRequest.setFrom(from);

        List<IndexHistory> out = new ArrayList<>();
        out.add(new IndexHistory());

        when(historyRepository.findAll(any(Specification.class), any(Pageable.class))).thenReturn(Page.empty());
        List<Map<String, Object>> result = indexHistoryAdapter.findByCollectionWithJobType(clusterId, historyReadRequest);
        verify(historyRepository, times(1)).findAll(any(Specification.class), any(Pageable.class));
        Assertions.assertEquals(0, result.size());
    }

}
