package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class AuthUser implements Serializable {

    private String sessionId;
    private User user;
    private Role role;
    private Cluster cluster;

    public AuthUser(String sessionId, User user, Role role) {
        this.sessionId = sessionId;
        this.user = user;
        this.role = role;
    }

    public AuthUser(String sessionId, User user, Role role, Cluster cluster) {
        this.sessionId = sessionId;
        this.user = user;
        this.role = role;
        this.cluster = cluster;
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

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }
}
