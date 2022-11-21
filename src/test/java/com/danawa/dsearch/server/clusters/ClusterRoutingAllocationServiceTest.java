package com.danawa.dsearch.server.clusters;

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
    private ElasticsearchFactory elasticsearchFactory;

    private ClusterRoutingAllocationService clusterRoutingAllocationService;

    @BeforeEach
    public void setup() {
        this.clusterRoutingAllocationService = new ClusterRoutingAllocationService(elasticsearchFactory);
    }

    @Test
    @DisplayName("ES 클러스터 샤드 분배 성공")
    public void allocate_param_is_all_success() throws IOException {
        UUID uuid = UUID.randomUUID();
        // "none" or "all"
        String allocate = "all";
        RestHighLevelClient client = mock(RestHighLevelClient.class);
        ClusterClient clusterClient = mock(ClusterClient.class);
        ClusterUpdateSettingsResponse response = mock(ClusterUpdateSettingsResponse.class);
        given(elasticsearchFactory.getClient(uuid)).willReturn(client);
        given(client.cluster()).willReturn(clusterClient);
        given(client.cluster().putSettings(any(ClusterUpdateSettingsRequest.class), eq(RequestOptions.DEFAULT))).willReturn(response);

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);

        Assertions.assertTrue(result);
    }
    @Test
    @DisplayName("ES 클러스터 샤드 분배 정지 성공")
    public void allocate_param_is_none_success() throws IOException {
        UUID uuid = UUID.randomUUID();
        // "none" or "all"
        String allocate = "none";
        RestHighLevelClient client = mock(RestHighLevelClient.class);
        ClusterClient clusterClient = mock(ClusterClient.class);
        ClusterUpdateSettingsResponse response = mock(ClusterUpdateSettingsResponse.class);
        given(elasticsearchFactory.getClient(uuid)).willReturn(client);
        given(client.cluster()).willReturn(clusterClient);
        given(client.cluster().putSettings(any(ClusterUpdateSettingsRequest.class), eq(RequestOptions.DEFAULT))).willReturn(response);

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);

        Assertions.assertTrue(result);
    }
    @Test
    @DisplayName("ES 클러스터 샤드 분배 여부에 이상한 문자열로 들어왔을 경우 실패")
    public void allocate_param_is_others_fail() throws IOException {
        UUID uuid = UUID.randomUUID();
        // "none" or "all"
        String allocate = "localhost";

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName("ES 클러스터와 연결 시 IOException 발생")
    public void elasticsearch_raise_io_exception() throws IOException {
        UUID uuid = UUID.randomUUID();
        // "none" or "all"
        String allocate = "none";
        RestHighLevelClient client = mock(RestHighLevelClient.class);
        ClusterClient clusterClient = mock(ClusterClient.class);
        given(elasticsearchFactory.getClient(uuid)).willReturn(client);
        given(client.cluster()).willReturn(clusterClient);
        given(client.cluster().putSettings(any(ClusterUpdateSettingsRequest.class), eq(RequestOptions.DEFAULT))).willThrow(new IOException());

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);
        Assertions.assertFalse(result);
    }
}
