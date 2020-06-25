package com.danawa.fastcatx.server.repository;

import com.danawa.fastcatx.server.entity.Cluster;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ClusterRepository extends JpaRepository<Cluster, UUID> {


}
