package com.danawa.fastcatx.server.entity;

import java.io.Serializable;

public class JdbcDeleteRequest implements Serializable {
    private String id;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }
}
