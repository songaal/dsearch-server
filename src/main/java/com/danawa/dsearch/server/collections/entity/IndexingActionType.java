package com.danawa.dsearch.server.collections.entity;

public enum IndexingActionType{
    ALL("all"),
    INDEXING("indexing"),
    EXPOSE("expose"),
    STOP_PROPAGATION("stop_propagation"),
    STOP_INDEXING("stop_indexing"),
    SUB_START("sub_start"),
    UNKNOWN(""),
    STOP_REINDEXING("stop_reindexing");

    private String action;

    IndexingActionType(String action){
        this.action = action;
    }

    public String getAction() {
        return action;
    }
}
