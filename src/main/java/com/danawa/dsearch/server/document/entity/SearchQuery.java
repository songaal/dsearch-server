package com.danawa.dsearch.server.document.entity;

import java.io.Serializable;

public class SearchQuery implements Serializable {
    private String id;
    private String name;
    private String index;
    private String query;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String indices) {
        this.index = indices;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
