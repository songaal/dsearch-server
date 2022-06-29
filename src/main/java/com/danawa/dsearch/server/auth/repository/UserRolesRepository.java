package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.auth.entity.UserRoles;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRolesRepository extends JpaRepository<UserRoles, Long> {
}
