package com.danawa.dsearch.server.dynamicIndex.service;

import com.danawa.dsearch.server.dynamicIndex.adapter.DynamicIndexAdapter;
import com.danawa.dsearch.server.dynamicIndex.dto.DynamicIndexInfoResponse;
import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DynamicIndexService {
    private static Logger logger = LoggerFactory.getLogger(DynamicIndexService.class);

    private DynamicIndexAdapter adapter;

    private QueueIndexerClient queueIndexerClient;

    public DynamicIndexService(DynamicIndexAdapter adapter,
                               QueueIndexerClient queueIndexerClient) {
        this.adapter = adapter;
        this.queueIndexerClient = queueIndexerClient;
    }

    public List<DynamicIndexInfoResponse> findAll(){
        List<DynamicIndexInfo> dynamicIndexInfoList = adapter.findAll();

        if (dynamicIndexInfoList.size() > 0) {
            return dynamicIndexInfoList.stream().map(dynamicInfo -> DynamicIndexInfoResponse.of(dynamicInfo)).collect(Collectors.toList());
        }

        return new ArrayList<DynamicIndexInfoResponse>();
    }

    public Map<String, List<DynamicIndexInfoResponse>> findBundleAll(){
        Map<String, List<DynamicIndexInfoResponse>> result = new HashMap<>();

        List<DynamicIndexInfoResponse> dynamicInfoList = findAll();
        for (DynamicIndexInfoResponse dynamicIndexInfoResponse : dynamicInfoList) {
            // 큐 인덱서에 대한 서버 별 묶음 정렬
            String bundleServer = dynamicIndexInfoResponse.getBundleServer();
            List<DynamicIndexInfoResponse> subList = result.getOrDefault(bundleServer, new ArrayList<>());
            subList.add(dynamicIndexInfoResponse);
            result.put(bundleServer, subList);
        }

        return result;
    }

    @Transactional
    public int resetAll(List<DynamicIndexInfo> dynamicIndexInfoList) {
        int result = 0;

        try {
            if (dynamicIndexInfoList.size() > 0) {
                deleteAll();
                List<DynamicIndexInfo> savedList = saveAll(dynamicIndexInfoList);
                result = savedList.size();
            }
        } catch (Exception e) {
            logger.error("", e);
        }

        return result;
    }

    private void deleteAll(){
        List<DynamicIndexInfo> allList = adapter.findAll();
        adapter.deleteAll(allList);
        adapter.flush();
    }

    private List<DynamicIndexInfo> saveAll(List<DynamicIndexInfo> dynamicIndexInfoList){
        return adapter.saveAll(dynamicIndexInfoList);
    }

    public Map<String, Integer> getState(long id) {
        Map<String, Integer> result = new HashMap<>();
        DynamicIndexInfo dynamicIndexInfo = adapter.findById(id);

        if (dynamicIndexInfo != null) {
            return getQueueIndexerState(dynamicIndexInfo);
        } else {
            logger.info("Not Found Id {}", id);
        }

        return result;
    }

    public Map<Long, Object> getStateAll() {
        Map<Long, Object> result = new HashMap<>();
        List<DynamicIndexInfo> dynamicIndexInfoList = adapter.findAll();

        for (DynamicIndexInfo dynamicIndexInfo : dynamicIndexInfoList) {
            result.put(dynamicIndexInfo.getId(), getQueueIndexerState(dynamicIndexInfo));
        }

        return result;
    }

    private Map<String, Integer> getQueueIndexerState(DynamicIndexInfo dynamicIndexInfo) {
        Map<String, Integer> result = new HashMap<>();

        try {
            URI url = URI.create(String.format("%s://%s:%s/%s", "http", dynamicIndexInfo.getIp(), dynamicIndexInfo.getPort(), dynamicIndexInfo.getStateEndPoint()));
            Map<String, Object> body = queueIndexerClient.get(url);

            int totalSize = 0;
            if (body.get("consume") != null) {
                Map<String, Object> queueMap = (Map) body.get("consume");

                for (String queueName : queueMap.keySet()) {
                    List<String> consumeIdList = (List) queueMap.get(queueName);
                    result.put(queueName, consumeIdList.size());
                    totalSize += consumeIdList.size();
                }
                result.put("count", totalSize);
            } else {
                logger.error("Queue Indexer Not Result {}", dynamicIndexInfo.getIp() + dynamicIndexInfo.getPort() + dynamicIndexInfo.getStateEndPoint());
            }
        } catch (Exception e) {
            logger.error("", e);
        }
        return result;
    }

    public int consumeAll(long id, HashMap<String, Object> body) {
        /**
         * 큐 인덱서에서 사용하는 rabbitmq consume 워커 갯수 조절
         */
        int result = 0;
        DynamicIndexInfo dynamicIndexInfo = adapter.findById(id);

        URI url = URI.create(String.format("%s://%s:%s/%s", "http", dynamicIndexInfo.getIp(), dynamicIndexInfo.getPort(), dynamicIndexInfo.getConsumeEndPoint()));
        Map<String, Object> stateMap = queueIndexerClient.put(url, body);

        if (stateMap.get("status") != null) {
            result = (int) stateMap.get("status");
        } else {
            logger.error("Queue Indexer Not Result {}", dynamicIndexInfo.getIp() + dynamicIndexInfo.getPort() + dynamicIndexInfo.getConsumeEndPoint());
        }

        return result;
    }
}
