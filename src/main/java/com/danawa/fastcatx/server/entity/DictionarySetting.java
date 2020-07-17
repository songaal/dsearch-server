package com.danawa.fastcatx.server.entity;
import java.io.Serializable;
import java.util.List;

public class DictionarySetting implements Serializable {
    private String documentId;
    private String id;
    private String name;
    private String type;
    private String tokenType;
    private String ignoreCase;
    private String updatedTime;
    private String appliedTime;
    private List<Column> columns;

    public static class Column {
        private String type;
        private String label;

        public Column() { }

        public Column(String type, String name) {
            this.type = type;
            this.label = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }
    }

    public String getAppliedTime() {
        return appliedTime;
    }

    public void setAppliedTime(String appliedTime) {
        this.appliedTime = appliedTime;
    }

    public String getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(String updatedTime) {
        this.updatedTime = updatedTime;
    }

    public String getDocumentId() {
        return documentId;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }

    public String getIgnoreCase() {
        return ignoreCase;
    }

    public void setIgnoreCase(String ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
