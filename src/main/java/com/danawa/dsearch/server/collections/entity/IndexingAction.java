package com.danawa.dsearch.server.collections.entity;

public enum IndexingAction {
    ALL("all"),
    INDEXING("indexing"),
    EXPOSE("expose"),
    STOP_PROPAGATION("stop_propagation"),
    STOP_INDEXING("stop_indexing"),
    SUB_START("sub_start"),

    UNKNOWN(""),
    STOP_REINDEXING("stop_reindexing");

    private String action;

    IndexingAction(String action){
        this.action = action;
    }

    public String getAction() {
        return action;
    }
}
