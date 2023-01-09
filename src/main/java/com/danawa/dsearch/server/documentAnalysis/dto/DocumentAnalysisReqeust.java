package com.danawa.dsearch.server.documentAnalysis.dto;

import java.io.Serializable;

public class DocumentAnalysisReqeust implements Serializable {
    private String name;
    private String index;
    private String query;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
