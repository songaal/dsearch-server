package com.danawa.dsearch.server.collections.entity;

public enum IndexerStatus {
    SUCCESS ("SUCCESS"),
    RUNNING ("RUNNING"),
    STOP ("STOP"),

    ERROR ("ERROR"),
    UNKNOWN ("UNKNOWN");

    private final String name;
    IndexerStatus(String name){
        this.name = name;
    }

    public String toString(){
        return this.name;
    }

    public static IndexerStatus changeToStatus(String name){
        String statusStr = name.toUpperCase();
        if (statusStr.equals("SUCCESS")){
            return IndexerStatus.SUCCESS;
        }else if (statusStr.equals("STOP")){
            return IndexerStatus.STOP;
        }else if (statusStr.equals("ERROR")){
            return IndexerStatus.ERROR;
        }else if (statusStr.equals("RUNNING")){
            return IndexerStatus.RUNNING;
        }else{
            return IndexerStatus.UNKNOWN;
        }
    }
}
