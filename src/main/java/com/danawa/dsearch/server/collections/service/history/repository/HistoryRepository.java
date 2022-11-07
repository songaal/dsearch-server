package com.danawa.dsearch.server.collections.service.history.repository;

import com.danawa.dsearch.server.collections.entity.IndexHistory;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface HistoryRepository extends JpaRepository<IndexHistory, Long> {

    List<IndexHistory> findByClusterIdAndIndexAndStartTimeAndJobType(UUID clusterId,
                                   String index,
                                   long startTime,
                                   String jobType);

    List<IndexHistory> findByClusterIdAndIndexLike(UUID clusterId, String index, Pageable pageable);
    List<IndexHistory> findByClusterIdAndJobTypeAndIndexLike(UUID clusterId, String jobType, String index, Pageable pageable);

    long countByClusterIdAndIndexLike(UUID clusterId, String index);
}
