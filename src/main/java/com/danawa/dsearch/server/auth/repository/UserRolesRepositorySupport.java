package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.stereotype.Repository;

import static com.danawa.dsearch.server.entity.QUserRoles.userRoles;

@Repository
public class UserRolesRepositorySupport extends QuerydslRepositorySupport {

    private final JPAQueryFactory queryFactory;

    public UserRolesRepositorySupport(JPAQueryFactory queryFactory) {
        super(UserRoles.class);
        this.queryFactory = queryFactory;
    }

    public UserRoles findByUserId(Long id) {
        return queryFactory.selectFrom(userRoles)
                .where(userRoles.userId.eq(id))
                .fetchOne();
    }
}
