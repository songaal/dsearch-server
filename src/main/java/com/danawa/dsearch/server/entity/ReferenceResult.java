package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class ReferenceResult implements Serializable {

    private ReferenceTemp template;
    private DocumentPagination documents;
    private String query;

    public ReferenceResult() {
    }

    public ReferenceResult(ReferenceTemp template, DocumentPagination documents, String query) {
        this.template = template;
        this.documents = documents;
        this.query = query;
    }

    public ReferenceTemp getTemplate() {
        return template;
    }

    public void setTemplate(ReferenceTemp template) {
        this.template = template;
    }

    public DocumentPagination getDocuments() {
        return documents;
    }

    public void setDocuments(DocumentPagination documents) {
        this.documents = documents;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
