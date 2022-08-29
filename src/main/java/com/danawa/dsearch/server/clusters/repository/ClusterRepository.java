package com.danawa.dsearch.server.clusters.repository;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ClusterRepository extends JpaRepository<Cluster, UUID> {

    List<Cluster> findByHostAndPort(String host, int port);
}
