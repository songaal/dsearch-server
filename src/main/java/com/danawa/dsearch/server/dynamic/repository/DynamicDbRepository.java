package com.danawa.dsearch.server.dynamic.repository;

import com.danawa.dsearch.server.dynamic.entity.DynamicInfo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DynamicDbRepository extends JpaRepository<DynamicInfo, Long> {
}
