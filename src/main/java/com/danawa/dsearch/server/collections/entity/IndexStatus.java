package com.danawa.dsearch.server.collections.entity;

import javax.annotation.Generated;
import javax.persistence.*;
import java.util.UUID;

@Entity
@SequenceGenerator(
        name="STATUS_SEQ_GEN", //시퀀스 제너레이터 이름
        sequenceName="STATUS_SEQ", //시퀀스 이름
        initialValue=1, //시작값
        allocationSize=1 //메모리를 통해 할당할 범위 사이즈
)
public class IndexStatus {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "STATUS_SEQ_GEN")
    private Long id;

    private UUID clusterId;
    private String collectionId;
    private String index;
    private long startTime;
    private String status;
    private String step;
    private String jobId;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCollectionId() {
        return collectionId;
    }

    public void setCollectionId(String collectionId) {
        this.collectionId = collectionId;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStep() {
        return step;
    }

    public void setStep(String step) {
        this.step = step;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public UUID getClusterId() {
        return clusterId;
    }

    public void setClusterId(UUID clusterId) {
        this.clusterId = clusterId;
    }
}
