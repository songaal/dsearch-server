package com.danawa.dsearch.server.clusters.entity;

import java.util.Map;

public class ClusterStatusResponse {

    private boolean connection;
    private Map<String, Object> nodes;
    private Map<String, Object> state;

    public ClusterStatusResponse() {
    }
    public ClusterStatusResponse(boolean connection, Map<String, Object> nodes, Map<String, Object> state) {
        this.connection = connection;
        this.nodes = nodes;
        this.state = state;
    }

    public boolean isConnection() {
        return connection;
    }

    public Map<String, Object> getNodes() {
        return nodes;
    }

    public Map<String, Object> getState() {
        return state;
    }

}
