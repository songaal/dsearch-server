package com.danawa.dsearch.server.config;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;

public class StubElasticsearchFactory extends ElasticsearchFactory {
    public StubElasticsearchFactory(ClusterService clusterService) {
        super(clusterService);
    }
}
