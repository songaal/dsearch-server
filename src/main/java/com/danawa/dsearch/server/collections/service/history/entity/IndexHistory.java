package com.danawa.dsearch.server.collections.service.history.entity;

import javax.persistence.*;
import java.util.UUID;

@Entity
@SequenceGenerator(
        name="HISTORY_SEQ_GEN", //시퀀스 제너레이터 이름
        sequenceName="HISTORY_SEQ", //시퀀스 이름
        initialValue=1, //시작값
        allocationSize=1 //메모리를 통해 할당할 범위 사이즈
)
public class IndexHistory {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "HISTORY_SEQ_GEN")
    private Long id;

    private UUID clusterId;
    private String index;
    private String jobType;
    private long startTime;
    private long endTime;
    private boolean autoRun;
    private String status;
    private String docSize;
    private String store;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public UUID getClusterId() {
        return clusterId;
    }

    public void setClusterId(UUID clusterId) {
        this.clusterId = clusterId;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
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

    public boolean isAutoRun() {
        return autoRun;
    }

    public void setAutoRun(boolean autoRun) {
        this.autoRun = autoRun;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDocSize() {
        return docSize;
    }

    public void setDocSize(String docSize) {
        this.docSize = docSize;
    }

    public String getStore() {
        return store;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public String toString(){
        return  "id=" + id + ", clusterId=" + clusterId.toString() + ", index=" + index + ", jobType=" + jobType + ", startTime=" + startTime
                + ", endTime=" + endTime + ", autoRun=" + autoRun + ", status=" + status + ", docSize=" + docSize +", store=" + store;
    }
}
