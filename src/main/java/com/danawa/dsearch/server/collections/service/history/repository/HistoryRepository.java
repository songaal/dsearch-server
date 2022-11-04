package com.danawa.dsearch.server.collections.service.history.repository;

import com.danawa.dsearch.server.collections.entity.IndexHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface HistoryRepository extends JpaRepository<IndexHistory, Long> {

//    @Query("SELECT h FROM History h WHERE h.clusterId = :clusterId and h.index = :index and h.startTime = :startTime and h.jobType = :jobType")
    List<IndexHistory> findByClusterIdAndIndexAndStartTimeAndJobType(UUID clusterId,
                                   String index,
                                   long startTime,
                                   String jobType);
}
