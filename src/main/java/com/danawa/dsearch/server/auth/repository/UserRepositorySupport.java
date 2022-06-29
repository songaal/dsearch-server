package com.danawa.dsearch.server.auth.repository;

import com.danawa.dsearch.server.auth.entity.User;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.stereotype.Repository;

import java.util.List;

import static com.danawa.dsearch.server.auth.entity.QUser.user;

@Repository
public class UserRepositorySupport extends QuerydslRepositorySupport {

    private final JPAQueryFactory queryFactory;

    public UserRepositorySupport(JPAQueryFactory queryFactory) {
        super(User.class);
        this.queryFactory = queryFactory;
    }

    public List<User> findAllByUsername(String username) {
        return queryFactory
                .selectFrom(user)
                .where(user.username.eq(username))
                .fetch();
    }

    public User findByEmail(String email) {
        return queryFactory
                .selectFrom(user)
                .where(user.email.eq(email))
                .fetchOne();
    }

}
