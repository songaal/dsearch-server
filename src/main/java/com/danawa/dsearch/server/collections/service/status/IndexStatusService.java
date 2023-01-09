package com.danawa.dsearch.server.collections.service.status;

import com.danawa.dsearch.server.collections.entity.IndexingInfo;
import com.danawa.dsearch.server.collections.service.status.entity.IndexStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class IndexStatusService implements StatusService {
    /**
     * 색인 상태에 대해서 관리하는 클래스 입니다.
     */

    private static final Logger logger = LoggerFactory.getLogger(IndexStatusService.class);

    private IndexStatusAdapter indexStatusAdapter;

    public IndexStatusService(
                              IndexStatusAdapter indexStatusAdapter) {
        this.indexStatusAdapter = indexStatusAdapter;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
//        indicesService.createSystemIndex(clusterId, lastIndexStatusIndex, lastIndexStatusIndexJson);
    }

    @Override
    public void create(IndexingInfo status, String currentStatus) {
        UUID clusterId = status.getClusterId();
        String collectionId = status.getCollection().getId();
        long startTime = status.getStartTime();
        String index = status.getIndex();
        String step = status.getCurrentStep().name();
        String jobId = status.getIndexingJobId();

        IndexStatus indexStatus = makeStatusEntity(clusterId, collectionId, index, startTime, currentStatus, step, jobId);
        indexStatusAdapter.create(indexStatus);
    }

    private IndexStatus makeStatusEntity(UUID clusterId, String collectionId, String index, long startTime, String status, String step, String jobId){
        IndexStatus indexStatus = new IndexStatus();
        indexStatus.setClusterId(clusterId);
        indexStatus.setCollectionId(collectionId);
        indexStatus.setIndex(index);
        indexStatus.setStartTime(startTime);
        indexStatus.setStatus(status);
        indexStatus.setStep(step);
        indexStatus.setJobId(jobId);
        return indexStatus;
    }

    @Override
    public void delete(UUID clusterId, String index, long startTime) {
        indexStatusAdapter.delete(clusterId, index, startTime);
    }

    @Override

    public void update(IndexingInfo indexingInfo, String status) {
        UUID clusterId = indexingInfo.getClusterId();
        String index = indexingInfo.getIndex();
        long startTime = indexingInfo.getStartTime();

        delete(clusterId, index, startTime);
        create(indexingInfo, status);
    }

    @Override
    public List<Map<String, Object>> findAll(UUID clusterId, int size, int from) {
        List<IndexStatus> list = indexStatusAdapter.findAll(clusterId, size, from);
        return convertToList(list);
    }

    private List<Map<String, Object>> convertToList(List<IndexStatus> list){
        List<Map<String, Object>> result = new ArrayList<>();

        for (IndexStatus entity: list){
            Map<String, Object> item = new HashMap<>();
            item.put("jobId", entity.getJobId());
            item.put("index", entity.getIndex());
            item.put("startTime", entity.getStartTime());
            item.put("step", entity.getStep());
            result.add(item);
        }

        return result;
    }
}
