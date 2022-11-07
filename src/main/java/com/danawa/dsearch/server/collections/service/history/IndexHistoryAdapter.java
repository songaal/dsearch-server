package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.service.history.repository.HistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class IndexHistoryAdapter {
    private HistoryRepository historyRepository;

    private static final Logger logger = LoggerFactory.getLogger(IndexHistoryAdapter.class);

    public IndexHistoryAdapter(HistoryRepository historyRepository){
        this.historyRepository = historyRepository;
    }

    public void create(IndexHistory indexHistory){
        IndexHistory savedIndexHistory = historyRepository.save(indexHistory);
        logger.info("saved History: {}", savedIndexHistory);
    }

    public long count(UUID clusterId, String index, long startTime, String jobType){
        List<IndexHistory> select = historyRepository.findByClusterIdAndIndexAndStartTimeAndJobType(clusterId, index, startTime, jobType);
        logger.info("{} {} {} History count: {}", clusterId, index, startTime, select.size());
        return select.size();
    }

    public long countByClusterIdAndIndex(UUID clusterId, HistoryReadRequest historyReadRequest){
        int len = 0 ;
        long count = 0;
        String collectionName = "";
        if(historyReadRequest.getIndexA() != null){
            len = historyReadRequest.getIndexA().length();
            collectionName = historyReadRequest.getIndexA().substring(0, len);
            count = historyRepository.countByClusterIdAndIndexLike(clusterId, collectionName);
        }else if(historyReadRequest.getIndexB() != null){
            len = historyReadRequest.getIndexA().length();
            collectionName = historyReadRequest.getIndexA().substring(0, len);
            count = historyRepository.countByClusterIdAndIndexLike(clusterId, collectionName);
        }

        logger.info("{} {} {} History count: {}", clusterId, collectionName, count);
        return count;
    }

    public List<Map<String,Object>> findAllByIndexs(UUID clusterId, HistoryReadRequest historyReadRequest){
        int len = historyReadRequest.getIndexA().length();
        String collectionName = historyReadRequest.getIndexA().substring(0, len);
        int from = historyReadRequest.getFrom();
        int size = historyReadRequest.getSize();
        Pageable paging = PageRequest.of(from, size);
        List<IndexHistory> list =  historyRepository.findByClusterIdAndIndexLike(clusterId, collectionName, paging);
        List<Map<String, Object>> result = new ArrayList<>();

        for (IndexHistory history: list){
            Map<String, Object> item = new HashMap<String, Object>();
            item.put("index", history.getIndex());
            item.put("jobType", history.getJobType());
            item.put("startTime", history.getStartTime());
            item.put("endTime", history.getEndTime());
            item.put("docSize", history.getDocSize());
            item.put("status", history.getStatus());
            item.put("store", history.getStore());
            item.put("autoRun", history.isAutoRun());
            result.add(item);
        }
        return result;
    }

    public List<Map<String,Object>> findAllByIndexsWithJobType(UUID clusterId, HistoryReadRequest historyReadRequest){
        int len = historyReadRequest.getIndexA().length();
        String collectionName = historyReadRequest.getIndexA().substring(0, len);

        int from = historyReadRequest.getFrom();
        int size = historyReadRequest.getSize();
        Pageable paging = PageRequest.of(from, size);
        String jobType = historyReadRequest.getJobType();
        List<IndexHistory> list =  historyRepository.findByClusterIdAndJobTypeAndIndexLike(clusterId, jobType, collectionName, paging);

        List<Map<String, Object>> result = new ArrayList<>();

        for (IndexHistory history: list){
            Map<String, Object> item = new HashMap<String, Object>();
            item.put("index", history.getIndex());
            item.put("jobType", history.getJobType());
            item.put("startTime", history.getStartTime());
            item.put("endTime", history.getEndTime());
            item.put("docSize", history.getDocSize());
            item.put("status", history.getStatus());
            item.put("store", history.getStore());
            item.put("autoRun", history.isAutoRun());
            result.add(item);
        }
        return result;
    }

}
