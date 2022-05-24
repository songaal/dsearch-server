package com.danawa.dsearch.server.clusters.repository;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ClusterRepository extends JpaRepository<Cluster, UUID> {

    List<Cluster> findByHostAndPort(String host, int port);
}
