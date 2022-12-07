package com.danawa.dsearch.server.clusters.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class ClusterRoutingAllocationService {

    private final ElasticsearchFactory elasticsearchFactory;

    public ClusterRoutingAllocationService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public boolean updateClusterAllocation(UUID clusterId, String allocate) {
       if(isWrongOption(allocate)){
           return false;
       }

        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.transientSettings(Settings.builder().put("cluster.routing.allocation.enable", allocate));
        // 정상적으로 호출되었을때만 response에 객체가 생성됨
        try {
            client.cluster().putSettings(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            return false;
        }
        return true;
    }
    private boolean isWrongOption(String allocate){
        if(allocate.equals("none") || allocate.equals("all")){
            return false;
        }
        return true;
    }
}
