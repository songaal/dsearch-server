package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.auth.adapter.ClusterRoutingAllocationAdapter;
import com.danawa.dsearch.server.clusters.service.ClusterRoutingAllocationService;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.ClusterClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class ClusterRoutingAllocationServiceTest {
    @Mock
    private ClusterRoutingAllocationAdapter adapter;

    private ClusterRoutingAllocationService clusterRoutingAllocationService;

    @BeforeEach
    public void setup() {
        this.clusterRoutingAllocationService = new ClusterRoutingAllocationService(adapter);
    }

    @Test
    @DisplayName("ES 클러스터 샤드 분배 성공")
    public void allocate_param_is_all_success() {
        UUID clusterId = UUID.randomUUID();
        given(adapter.updateClusterSettings(clusterId, "all")).willReturn(true);
        boolean result = clusterRoutingAllocationService.updateClusterAllocation(clusterId, true);
        Assertions.assertTrue(result);
    }
    @Test
    @DisplayName("ES 클러스터 샤드 분배 정지 성공")
    public void allocate_param_is_none_success() {
        UUID clusterId = UUID.randomUUID();
        given(adapter.updateClusterSettings(clusterId, "none")).willReturn(true);
        boolean result = clusterRoutingAllocationService.updateClusterAllocation(clusterId, false);
        Assertions.assertTrue(result);
    }
}
