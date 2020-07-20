package com.danawa.fastcatx.server.entity;

import java.io.Serializable;
import java.util.Queue;
import java.util.Set;

public class IndexingStatus implements Serializable {
    private String collectionId;
    private String host;
    private int port;
    private String index;
    private String indexingJobId;
    private boolean isAutoRun;


    private String status;
    private String error;
    private long startTime;
    private long endTime;
    private String action;

    private int retry;
    private IndexStep currentStep;
    private Queue<IndexStep> nextStep;

    public String getCollectionId() {
        return collectionId;
    }

    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
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

    public IndexStep getCurrentStep() {
        return currentStep;
    }

    public void setCurrentStep(IndexStep currentStep) {
        this.currentStep = currentStep;
    }

    public Queue<IndexStep> getNextStep() {
        return nextStep;
    }

    public void setNextStep(Queue<IndexStep> nextStep) {
        this.nextStep = nextStep;
    }
}
