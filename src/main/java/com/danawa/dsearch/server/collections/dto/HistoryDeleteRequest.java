package com.danawa.dsearch.server.collections.dto;

public class HistoryDeleteRequest {
    private String collectionName;

    private Long time;

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString(){
        return  "collectionName=" + collectionName +", time=" + time;
    }
}
