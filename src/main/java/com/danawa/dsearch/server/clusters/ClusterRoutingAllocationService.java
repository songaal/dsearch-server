package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
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

    public ClusterUpdateSettingsResponse updateClusterAllocation(UUID clusterId, String allocate) throws IOException {
        RestHighLevelClient client = elasticsearchFactory.getClient(clusterId);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.transientSettings(Settings.builder().put("cluster.routing.allocation.enable", allocate));
        // 정상적으로 호출되었을때만 response에 객체가 생성됨
        return client.cluster().putSettings(request, RequestOptions.DEFAULT);
    }
}
