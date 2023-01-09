package com.danawa.dsearch.server.auth.adapter;

import com.danawa.dsearch.server.auth.repository.ClusterRoutingAllocationRepository;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class ClusterRoutingAllocationAdapter {

    private ClusterRoutingAllocationRepository repository;

    public ClusterRoutingAllocationAdapter(ClusterRoutingAllocationRepository repository){
        this.repository = repository;
    }

    public boolean updateClusterSettings(UUID clusterId, String allocate){
        if(allocate.equals("all") || allocate.equals("none")){
            return repository.updateClusterSettings(clusterId, allocate);
        }else{
            return false;
        }
    }

}
