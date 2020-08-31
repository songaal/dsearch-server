package com.danawa.dsearch.server.repository;

import com.danawa.dsearch.server.entity.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ClusterRepository extends JpaRepository<Cluster, UUID> {


}
