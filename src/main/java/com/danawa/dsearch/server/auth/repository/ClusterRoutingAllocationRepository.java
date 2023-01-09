package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class ClusterRoutingAllocationRepository {

    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    public ClusterRoutingAllocationRepository(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper) {
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    public boolean updateClusterSettings(UUID clusterId, String allocate){
        try{
            ClusterUpdateSettingsResponse response = elasticsearchFactoryHighLevelWrapper.updateClusterSettings(clusterId, allocate);
            return response.isAcknowledged();
        }catch (IOException e) {
            return false;
        }
    }

}
