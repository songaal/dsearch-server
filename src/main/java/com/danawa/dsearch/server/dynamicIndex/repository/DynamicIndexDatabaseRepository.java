package com.danawa.dsearch.server.dynamicIndex.repository;

import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DynamicIndexDatabaseRepository extends JpaRepository<DynamicIndexInfo, Long> {
}
