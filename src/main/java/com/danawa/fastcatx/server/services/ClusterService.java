package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.Cluster;
import com.danawa.fastcatx.server.excpetions.NotFoundException;
import com.danawa.fastcatx.server.repository.ClusterRepository;
import com.danawa.fastcatx.server.repository.ClusterRepositorySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ClusterService {
    private static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private final ClusterRepository clusterRepository;
    private final ClusterRepositorySupport clusterRepositorySupport;

    public ClusterService(ClusterRepository clusterRepository,
                          ClusterRepositorySupport clusterRepositorySupport) {
        this.clusterRepository = clusterRepository;
        this.clusterRepositorySupport = clusterRepositorySupport;
    }

    public List<Cluster> findAll() {
        return clusterRepository.findAll();
    }

    public Cluster find(Long id) {
        return clusterRepository.findById(id).get();
    }

    public Cluster add(Cluster cluster) {
        cluster.setId(null);
        return clusterRepository.save(cluster);
    }

    public Cluster remove(Long id) {
        Cluster registerCluster = clusterRepository.findById(id).get();
        clusterRepository.delete(registerCluster);
        return registerCluster;
    }

    public Cluster edit(Long id, Cluster cluster) throws NotFoundException {
        Cluster registerCluster = clusterRepository.findById(id).get();
        if (registerCluster == null) {
            throw new NotFoundException("Not Found Cluster");
        }
        registerCluster.setName(cluster.getName());
        registerCluster.setNodes(cluster.getNodes());
        return clusterRepository.save(registerCluster);
    }
}
