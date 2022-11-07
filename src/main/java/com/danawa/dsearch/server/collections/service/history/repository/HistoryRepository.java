package com.danawa.dsearch.server.collections.service.history.repository;

import com.danawa.dsearch.server.collections.entity.IndexHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface HistoryRepository extends JpaRepository<IndexHistory, Long> {

    List<IndexHistory> findByClusterIdAndIndexAndStartTimeAndJobType(
            String clusterId,
            String index,
            long startTime,
            String jobType);

    List<IndexHistory> findByClusterIdAndIndexStartsWith(String clusterId, String index, Pageable pageable);
    List<IndexHistory> findByClusterIdAndJobType(String clusterId, String jobType, Pageable pageable);
    List<IndexHistory> findByClusterIdAndJobTypeAndIndexStartsWith(String clusterId, String jobType, String index, Pageable pageable);

    Long countByClusterIdAndIndexStartsWith( String clusterId, String index);
    Long countByClusterId(String clusterId);

    void deleteByClusterIdAndIndexStartsWith(String clusterId, String collectionId);
}
