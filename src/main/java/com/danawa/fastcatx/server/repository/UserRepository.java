package com.danawa.fastcatx.server.repository;

import com.danawa.fastcatx.server.entity.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}
