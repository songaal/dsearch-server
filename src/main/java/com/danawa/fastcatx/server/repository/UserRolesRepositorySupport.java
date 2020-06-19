package com.danawa.fastcatx.server.repository;

import com.danawa.fastcatx.server.entity.UserRoles;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.stereotype.Repository;

@Repository
public class UserRolesRepositorySupport extends QuerydslRepositorySupport {

    private final JPAQueryFactory queryFactory;

    public UserRolesRepositorySupport(JPAQueryFactory queryFactory) {
        super(UserRoles.class);
        this.queryFactory = queryFactory;
    }


}
