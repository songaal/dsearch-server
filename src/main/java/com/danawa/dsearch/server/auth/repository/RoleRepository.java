package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.auth.entity.Role;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RoleRepository extends JpaRepository<Role, Long> {
}
