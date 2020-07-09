package com.danawa.fastcatx.server.entity;

import java.io.Serializable;
import java.util.List;

public class AnalyzerTokens implements Serializable {
    private String field;
    private List<String> tokens;

    public void setField(String field) {
        this.field = field;
    }
    public void setTokens(List<String> tokens) {
        this.tokens = tokens;
    }
    public String getField() {
        return field;
    }
    public List<String> getTokens() {
        return tokens;
    }


}
