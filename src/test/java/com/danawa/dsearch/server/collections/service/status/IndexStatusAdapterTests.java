package com.danawa.dsearch.server.collections.service.status;

import com.danawa.dsearch.server.collections.entity.IndexStatus;
import com.danawa.dsearch.server.collections.service.status.repository.StatusRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IndexStatusAdapterTests {

    private IndexStatusAdapter indexStatusAdapter;

    @Mock
    private StatusRepository statusRepository;

    @BeforeEach
    public void setup(){
        this.indexStatusAdapter = new IndexStatusAdapter(statusRepository);
    }

    @Test
    @DisplayName("상태 생성 테스트")
    public void createTests(){
        UUID clusterId = UUID.randomUUID();
        String index = "index";

        IndexStatus output = new IndexStatus();
        output.setId(1L);
        output.setIndex(index);
        output.setClusterId(clusterId);

        IndexStatus input = new IndexStatus();
        input.setIndex(index);
        input.setClusterId(clusterId);


        when(statusRepository.save(input)).thenReturn(output);
        indexStatusAdapter.create(input);
        verify(statusRepository, times(1)).save(input);

    }

    @Test
    @DisplayName("상태 제거 테스트")
    public void deleteTests(){
        UUID clusterId = UUID.randomUUID();
        String index = "index";
        long startTime = 1L;

        doNothing().when(statusRepository).deleteByClusterIdAndIndexAndStartTime(clusterId, index, startTime);
        indexStatusAdapter.delete(clusterId, index, startTime);
        verify(statusRepository, times(1)).deleteByClusterIdAndIndexAndStartTime(clusterId, index, startTime);
    }

    @Test
    @DisplayName("상태 찾기 테스트")
    public void findAllTests(){
        UUID clusterId = UUID.randomUUID();
        int size = 10;
        int from = 0;
        Pageable page = PageRequest.of(from, size);
        IndexStatus indexStatus = new IndexStatus();
        List<IndexStatus> output = new ArrayList();
        output.add(indexStatus);

        when(statusRepository.findByClusterId(clusterId, page)).thenReturn(output);
        List<IndexStatus> result  = indexStatusAdapter.findAll(clusterId, size, from);
        verify(statusRepository, times(1)).findByClusterId(clusterId, page);
        Assertions.assertEquals(1, result.size());
    }



}
