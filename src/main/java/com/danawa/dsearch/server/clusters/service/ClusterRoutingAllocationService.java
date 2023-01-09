package com.danawa.dsearch.server.clusters.service;

import com.danawa.dsearch.server.auth.adapter.ClusterRoutingAllocationAdapter;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ClusterRoutingAllocationService {

    private ClusterRoutingAllocationAdapter allocationAdapter;

    public ClusterRoutingAllocationService(ClusterRoutingAllocationAdapter allocationAdapter) {
        this.allocationAdapter = allocationAdapter;
    }

    public boolean updateClusterAllocation(UUID clusterId, boolean isAllocation) {
        String allocation = convertAllocationOption(isAllocation);
        return allocationAdapter.updateClusterSettings(clusterId, allocation);
    }

    private String convertAllocationOption(boolean isAllocation){
        if(isAllocation){
            return "all";
        }else{
            return "none";
        }
    }
}
