package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.auth.entity.Role;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.stereotype.Repository;


@Repository
public class RoleRepositorySupport extends QuerydslRepositorySupport {

    private final JPAQueryFactory queryFactory;

    public RoleRepositorySupport(JPAQueryFactory queryFactory) {
        super(Role.class);
        this.queryFactory = queryFactory;
    }


}
