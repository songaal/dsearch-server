package com.danawa.dsearch.server.collections.entity;

import java.io.Serializable;
import java.util.Queue;
import java.util.UUID;

public class IndexingInfo implements Serializable {
    public static IndexingInfo Empty = null;
    private Collection collection;
    private String scheme;
    private String host;
    private int port;
    private String index;
    private String indexingJobId;
    private boolean isAutoRun;
    private UUID clusterId;

    private String taskId;

    private String status;
    private String error;
    private long startTime;
    private long endTime;
    private String action;

    private int retry;
    private IndexingStep currentStep;
    private Queue<IndexingStep> nextStep;

    public void setCollection(Collection collection) {
        this.collection = collection;
    }

    public Collection getCollection() {
        return collection;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getIndexingJobId() {
        return indexingJobId;
    }

    public void setIndexingJobId(String indexingJobId) {
        this.indexingJobId = indexingJobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public boolean isAutoRun() {
        return isAutoRun;
    }

    public void setAutoRun(boolean autoRun) {
        isAutoRun = autoRun;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public IndexingStep getCurrentStep() {
        return currentStep;
    }

    public void setCurrentStep(IndexingStep currentStep) {
        this.currentStep = currentStep;
    }

    public Queue<IndexingStep> getNextStep() {
        return nextStep;
    }

    public void setNextStep(Queue<IndexingStep> nextStep) {
        this.nextStep = nextStep;
    }

    public UUID getClusterId() {
        return clusterId;
    }

    public void setClusterId(UUID clusterId) {
        this.clusterId = clusterId;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public String toString() {
        return "IndexingStatus{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", index='" + index + '\'' +
                ", indexingJobId='" + indexingJobId + '\'' +
                ", isAutoRun=" + isAutoRun +
                ", clusterId=" + clusterId +
                ", taskId=" + taskId +
                ", status='" + status + '\'' +
                ", error='" + error + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", action='" + action + '\'' +
                ", retry=" + retry +
                ", currentStep=" + currentStep +
                ", nextStep=" + nextStep +
                '}';
    }
}
