package com.danawa.dsearch.server.auth.entity;

public class UserProfile {
    private String email;
    private String username;
    private long roleId;

    public UserProfile(long roleId, String username, String email){
        this.roleId = roleId;
        this.username = username;
        this.email = email;
    }

    public String getEmail() {
        return email;
    }

    public String getUsername() {
        return username;
    }

    public long getRoleId() {
        return roleId;
    }

}
