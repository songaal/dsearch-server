package com.danawa.dsearch.server.collections.dto;

public class HistoryReadRequest {
    private String indexA;
    private String indexB;
    private int from;
    private int size;
    private String jobType;

    public String getIndexA() {
        return indexA;
    }

    public void setIndexA(String indexA) {
        this.indexA = indexA;
    }

    public String getIndexB() {
        return indexB;
    }

    public void setIndexB(String indexB) {
        this.indexB = indexB;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    @Override
    public String toString(){
        return "indexA=" +indexA + ", indexB="+ indexB + ", size=" + size+ ", from="+ from;
    }
}
