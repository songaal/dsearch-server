package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.clusters.repository.ClusterRepository;
import com.danawa.dsearch.server.clusters.service.ClusterRoutingAllocationService;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class ClusterRoutingAllocationServiceTest {
    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    @InjectMocks
    private ClusterRoutingAllocationService clusterRoutingAllocationService;

    @BeforeEach
    public void setup() {
        this.clusterRoutingAllocationService = new FakeClusterRoutingAllocationService(elasticsearchFactory);
    }

    @Test
    public void allocate_param_is_all_success() {
        UUID uuid = UUID.randomUUID();
        // "none" or "all"
        String allocate = "all";

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);

        Assertions.assertTrue(result);
    }
    @Test
    public void allocate_param_is_none_success() {
        UUID uuid = UUID.randomUUID();
        // "none" or "all"
        String allocate = "none";

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);

        Assertions.assertTrue(result);
    }
    @Test
    public void allocate_param_is_others_fail() {
        UUID uuid = UUID.randomUUID();
        String allocate = "localhost";

        boolean result = clusterRoutingAllocationService.updateClusterAllocation(uuid, allocate);

        Assertions.assertFalse(result);
    }
}
