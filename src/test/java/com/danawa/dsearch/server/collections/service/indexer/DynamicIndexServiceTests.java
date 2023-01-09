package com.danawa.dsearch.server.collections.service.indexer;

import com.danawa.dsearch.server.dynamicIndex.adapter.DynamicIndexAdapter;
import com.danawa.dsearch.server.dynamicIndex.dto.DynamicIndexInfoResponse;
import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import com.danawa.dsearch.server.dynamicIndex.service.DynamicIndexService;
import com.danawa.dsearch.server.dynamicIndex.service.QueueIndexerClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class DynamicIndexServiceTests {
    private DynamicIndexService dynamicIndexService;

    @Mock
    private DynamicIndexAdapter adapter;
    @Mock
    private QueueIndexerClient queueIndexerClient;

    @BeforeEach
    public void setup() {
        this.dynamicIndexService = new DynamicIndexService(adapter, queueIndexerClient);
    }

    @Test
    @DisplayName("큐인덱서 정보 전체 조회 성공")
    public void findAll_success(){
        given(adapter.findAll()).willReturn(new ArrayList<>());
        List<DynamicIndexInfoResponse> result = dynamicIndexService.findAll();
        Assertions.assertEquals(result.size(), 0);
    }

}
