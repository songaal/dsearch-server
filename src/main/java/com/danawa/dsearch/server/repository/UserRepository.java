package com.danawa.dsearch.server.repository;

import com.danawa.dsearch.server.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}
