package com.danawa.dsearch.server.collections.service.history.repository;

import com.danawa.dsearch.server.collections.entity.IndexHistory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface HistoryRepository extends JpaRepository<IndexHistory, Long>, JpaSpecificationExecutor<IndexHistory> {

    Page<IndexHistory> findAll(Specification<IndexHistory> spec, Pageable pageable);

    Long countByClusterId(UUID clusterId);

    @Override
    long count(Specification<IndexHistory> spec);
}
