package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.auth.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}
