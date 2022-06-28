package com.danawa.dsearch.server.clusters;

import com.danawa.dsearch.server.clusters.entity.Cluster;

import java.util.ArrayList;
import java.util.List;

public class RepositoryHelper {
    public List<Cluster> getAllClutersForEmpty(){
        return new ArrayList<Cluster>();
    }

    public List<Cluster> getAllClutersJustOne(){
        Cluster cluster = new Cluster();
        List<Cluster> result = new ArrayList<Cluster>();
        result.add(cluster);
        return result;
    }

    public Cluster getEmptyCluster(){
        return null;
    }

    public Cluster getCluster(){
        return new Cluster();
    }
}
