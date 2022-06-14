package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.clusters.service.ClusterRoutingAllocationService;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.io.IOException;
import java.util.UUID;

public class FakeClusterRoutingAllocationService extends ClusterRoutingAllocationService {
    public FakeClusterRoutingAllocationService(ElasticsearchFactory elasticsearchFactory) {
        super(elasticsearchFactory);
    }

    public boolean updateClusterAllocation(UUID clusterId, String allocate) {
        if(allocate.equals("none") || allocate.equals("all")){
            return true;
        }else{
            return false;
        }
    }
}
