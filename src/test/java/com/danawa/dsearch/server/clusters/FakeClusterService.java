package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.clusters.dto.ClusterStatusRequest;
import com.danawa.dsearch.server.clusters.dto.ClusterStatusResponse;
import com.danawa.dsearch.server.clusters.repository.ClusterRepository;
import com.danawa.dsearch.server.clusters.service.ClusterService;

import java.util.HashMap;

public class FakeClusterService extends ClusterService {
    public FakeClusterService(ClusterRepository clusterRepository) {
        super(clusterRepository);
    }

    public ClusterStatusResponse scanClusterStatus(ClusterStatusRequest clusterStatusRequest) {
        if(clusterStatusRequest.getHost().equals("test") && clusterStatusRequest.getPort() == 9200){
            return new ClusterStatusResponse(true, new HashMap<>(),  new HashMap<>());
        } else{
            return new ClusterStatusResponse(false, new HashMap<>(),  new HashMap<>());
        }
    }
}