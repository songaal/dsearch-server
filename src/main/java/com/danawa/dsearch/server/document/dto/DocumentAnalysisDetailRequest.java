package com.danawa.dsearch.server.document.dto;

public class DocumentAnalysisDetailRequest {
    private String index;
    private String docId;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }
}
