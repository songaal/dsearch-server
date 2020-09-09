package com.danawa.dsearch.server.repository;

import com.danawa.dsearch.server.entity.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import javax.persistence.criteria.CriteriaBuilder;
import java.util.List;
import java.util.UUID;

public interface ClusterRepository extends JpaRepository<Cluster, UUID> {

    List<Cluster> findByHostAndPort(String host, int port);
}
