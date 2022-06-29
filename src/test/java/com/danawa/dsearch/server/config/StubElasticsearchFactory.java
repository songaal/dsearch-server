package com.danawa.dsearch.server.config;

import com.danawa.dsearch.server.clusters.service.ClusterService;

public class StubElasticsearchFactory extends ElasticsearchFactory {
    public StubElasticsearchFactory(ClusterService clusterService) {
        super(clusterService);
    }
}
