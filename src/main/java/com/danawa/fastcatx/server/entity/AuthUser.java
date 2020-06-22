package com.danawa.fastcatx.server.entity;

import java.io.Serializable;

public class AuthUser implements Serializable {

    private String sessionId;
    private User user;
    private Role role;


    public AuthUser(String sessionId, User user, Role role) {
        this.sessionId = sessionId;
        this.user = user;
        this.role = role;
    }

    public String getSessionId() {
        return sessionId;
    }

    public User getUser() {
        return user;
    }

    public Role getRole() {
        return role;
    }
}
