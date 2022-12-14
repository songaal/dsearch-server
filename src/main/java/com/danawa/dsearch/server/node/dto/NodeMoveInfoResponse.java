package com.danawa.dsearch.server.node.dto;

import java.io.Serializable;

public class NodeMoveInfoResponse implements Serializable {

    private String index;
    private String shard;
    private String time;
    private String type;
    private String stage;
    private String sourceNode;
    private String targetNode;
    private String files;
    private String filesRecovered;
    private String filesPercent;
    private String bytes;
    private String bytesRecovered;
    private String bytesPercent;
    private String bytesTotal;
    private String translogOps;
    private String translogOpsRecovered;
    private String translogOpsPercent;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getShard() {
        return shard;
    }

    public void setShard(String shard) {
        this.shard = shard;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public String getSourceNode() {
        return sourceNode;
    }

    public void setSourceNode(String sourceNode) {
        this.sourceNode = sourceNode;
    }

    public String getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(String targetNode) {
        this.targetNode = targetNode;
    }

    public String getFiles() {
        return files;
    }

    public void setFiles(String files) {
        this.files = files;
    }

    public String getFilesRecovered() {
        return filesRecovered;
    }

    public void setFilesRecovered(String filesRecovered) {
        this.filesRecovered = filesRecovered;
    }

    public String getFilesPercent() {
        return filesPercent;
    }

    public void setFilesPercent(String filesPercent) {
        this.filesPercent = filesPercent;
    }

    public String getBytes() {
        return bytes;
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
    }

    public String getBytesRecovered() {
        return bytesRecovered;
    }

    public void setBytesRecovered(String bytesRecovered) {
        this.bytesRecovered = bytesRecovered;
    }

    public String getBytesPercent() {
        return bytesPercent;
    }

    public void setBytesPercent(String bytesPercent) {
        this.bytesPercent = bytesPercent;
    }

    public String getBytesTotal() {
        return bytesTotal;
    }

    public void setBytesTotal(String bytesTotal) {
        this.bytesTotal = bytesTotal;
    }

    public String getTranslogOps() {
        return translogOps;
    }

    public void setTranslogOps(String translogOps) {
        this.translogOps = translogOps;
    }

    public String getTranslogOpsRecovered() {
        return translogOpsRecovered;
    }

    public void setTranslogOpsRecovered(String translogOpsRecovered) {
        this.translogOpsRecovered = translogOpsRecovered;
    }

    public String getTranslogOpsPercent() {
        return translogOpsPercent;
    }

    public void setTranslogOpsPercent(String translogOpsPercent) {
        this.translogOpsPercent = translogOpsPercent;
    }
}
