package com.danawa.dsearch.server.jdbc.dto;

import java.io.Serializable;

public class JdbcUpdateRequest implements Serializable {
    private String id;
    private String name;
    private String provider;
    private String driver;
    private String address;
    private String port;
    private String db_name;
    private String user;
    private String password;
    private String params;
    private String url;

    public String getId() {
        return id;
    }
    public String getName() {
        return name;
    }
    public String getProvider() {
        return provider;
    }
    public String getDriver() {
        return driver;
    }

    public String getAddress() {
        return address;
    }

    public String getPort() {
        return port;
    }
    public String getDB_name() {
        return db_name;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getParams() {
        return params;
    }

    public String getUrl() {
        return url;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public void setDB_name(String db_name) {
        this.db_name = db_name;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
