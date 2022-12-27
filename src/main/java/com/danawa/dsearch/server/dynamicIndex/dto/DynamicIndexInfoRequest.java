package com.danawa.dsearch.server.dynamicIndex.dto;

import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;

import java.io.Serializable;

public class DynamicIndexInfoRequest implements Serializable {
    private Long id;

    private String bundleQueue;
    private String bundleServer;
    private String scheme;
    private String ip;
    private String port;
    private String stateEndPoint;
    private String consumeEndPoint;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBundleQueue() {
        return bundleQueue;
    }

    public void setBundleQueue(String bundleQueue) {
        this.bundleQueue = bundleQueue;
    }

    public String getBundleServer() {
        return bundleServer;
    }

    public void setBundleServer(String bundleServer) {
        this.bundleServer = bundleServer;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getStateEndPoint() {
        return stateEndPoint;
    }

    public void setStateEndPoint(String stateEndPoint) {
        this.stateEndPoint = stateEndPoint;
    }

    public String getConsumeEndPoint() {
        return consumeEndPoint;
    }

    public void setConsumeEndPoint(String consumeEndPoint) {
        this.consumeEndPoint = consumeEndPoint;
    }


    public static DynamicIndexInfo to(DynamicIndexInfoRequest request) {
        DynamicIndexInfo info = new DynamicIndexInfo();
        info.setId(request.getId());
        info.setBundleQueue(request.getBundleQueue());
        info.setBundleServer(request.getBundleServer());
        info.setScheme(request.getScheme());
        info.setIp(request.getIp());
        info.setPort(request.getPort());
        info.setStateEndPoint(request.getStateEndPoint());
        info.setConsumeEndPoint(request.getConsumeEndPoint());
        return info;
    }
}
