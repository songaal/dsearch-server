package com.danawa.dsearch.server.collections.service.status.repository;

import com.danawa.dsearch.server.collections.entity.IndexStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;


@Repository
public interface StatusRepository extends JpaRepository<IndexStatus, Long> {
    void deleteByClusterIdAndIndexAndStartTime(UUID clusterId, String index, long startTime);

    List<IndexStatus> findByClusterId(UUID clusterId, Pageable pageable);
}
