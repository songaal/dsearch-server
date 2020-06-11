package com.danawa.fastcatx.server.entity;


import java.io.Serializable;

public class DictDocumentRequest implements Serializable {
    private String keyword;
    private String synonym;
    private String id; // 사전의 데이터 아이디
    private String type;

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getSynonym() {
        return synonym;
    }

    public void setSynonym(String synonym) {
        this.synonym = synonym;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
