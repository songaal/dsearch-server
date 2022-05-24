package com.danawa.dsearch.server.auth.entity;

import com.danawa.dsearch.server.clusters.entity.Cluster;

import java.io.Serializable;

public class AuthUser implements Serializable {
    private String token;
    private User user;
    private Role role;
    private Cluster cluster;

    public AuthUser(User user, Role role) {
        this.user = user;
        this.role = role;
    }

    public AuthUser(User user, Role role, Cluster cluster) {
        this.user = user;
        this.role = role;
        this.cluster = cluster;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
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
