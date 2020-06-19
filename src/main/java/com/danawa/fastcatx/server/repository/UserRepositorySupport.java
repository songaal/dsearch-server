package com.danawa.fastcatx.server.repository;

import com.danawa.fastcatx.server.entity.QUser;
import com.danawa.fastcatx.server.entity.User;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.stereotype.Repository;

import java.util.List;

import static com.danawa.fastcatx.server.entity.QUser.user;

@Repository
public class UserRepositorySupport extends QuerydslRepositorySupport {

    private final JPAQueryFactory queryFactory;

    public UserRepositorySupport(JPAQueryFactory queryFactory) {
        super(User.class);
        this.queryFactory = queryFactory;
    }

    public List<User> findAll(String username) {
        return queryFactory
                .selectFrom(user)
                .where(user.username.eq(username))
                .fetch();
    }

}
