package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.service.history.repository.HistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
        logger.info("saved History: {}, {}, {}", savedIndexHistory.getId(), savedIndexHistory.getClusterId(), savedIndexHistory.getIndex());
    }

    @Transactional
    public void deleteAll(UUID clusterId, String collectionId){
        try{
            historyRepository.deleteByClusterIdAndIndexStartsWith(clusterId, collectionId);
        }catch (Exception e){
            logger.info("", e);
        }
    }

    public long count(UUID clusterId, String index, long startTime, String jobType){
        List<IndexHistory> select = historyRepository.findByClusterIdAndIndexAndStartTimeAndJobType(clusterId, index, startTime, jobType);
        logger.info("{} {} {} History count: {}", clusterId, index, startTime, select.size());
        return select.size();
    }

    public long countByClusterIdAndIndex(UUID clusterId, HistoryReadRequest historyReadRequest){
        long count = 0;
        String collectionName = parseCollectionName(historyReadRequest);

        try{
            count = historyRepository.countByClusterIdAndIndexStartsWith(clusterId, collectionName);
        }catch (Exception e){
            logger.info("{}", e);
        }

        return count;
    }

    public long countByClusterId(UUID clusterId){
        long count = 0;

        try{
            count = historyRepository.countByClusterId(clusterId);
        }catch (Exception e){
            logger.info("{}", e);
        }

        return count;
    }

    public List<Map<String,Object>> findByCollection(UUID clusterId, HistoryReadRequest historyReadRequest){
        String collectionName = parseCollectionName(historyReadRequest);
        Pageable paging = getPagination(historyReadRequest.getFrom(), historyReadRequest.getSize());

        List<IndexHistory> list = new ArrayList<>();
        List<Map<String, Object>> result = new ArrayList<>();

        try{
            list =  historyRepository.findByClusterIdAndIndexStartsWith(clusterId, collectionName, paging);
        }catch (Exception e){
            logger.info("", e);
        }

        for (IndexHistory history: list){
            Map<String, Object> item = makeHistoryMap(history);
            result.add(item);
        }
        return result;
    }

    public List<Map<String,Object>> findByClusterIdAndJobType(UUID clusterId, HistoryReadRequest historyReadRequest){
        Pageable paging = getPagination(historyReadRequest.getFrom(), historyReadRequest.getSize());
        String jobType = historyReadRequest.getJobType();
        List<IndexHistory> list = new ArrayList<>();
        List<Map<String, Object>> result = new ArrayList<>();

        try{
            list =  historyRepository.findByClusterIdAndJobType(clusterId, jobType, paging);
        }catch (Exception e){
            logger.info("", e);
        }

        for (IndexHistory history: list){
            Map<String, Object> item = makeHistoryMap(history);
            result.add(item);
        }
        return result;
    }

    public List<Map<String,Object>> findByCollectionWithJobType(UUID clusterId, HistoryReadRequest historyReadRequest){

        String collectionName = parseCollectionName(historyReadRequest);
        String jobType = historyReadRequest.getJobType();

        Pageable paging = getPagination(historyReadRequest.getFrom(), historyReadRequest.getSize());

        List<IndexHistory> list = new ArrayList<>();
        List<Map<String, Object>> result = new ArrayList<>();
        try{
             list =  historyRepository.findByClusterIdAndJobTypeAndIndexStartsWith(clusterId, jobType, collectionName, paging);
        }catch (Exception e){
            logger.info("", e);
        }

        for (IndexHistory history: list){
            Map<String, Object> item = makeHistoryMap(history);
            result.add(item);
        }
        return result;
    }

    private Map<String, Object> makeHistoryMap(IndexHistory history){
        logger.info("{}", history);
        Map<String, Object> item = new HashMap<String, Object>();
        item.put("id", history.getId());
        item.put("index", history.getIndex());
        item.put("jobType", history.getJobType());
        item.put("startTime", history.getStartTime());
        item.put("endTime", history.getEndTime());
        item.put("docSize", history.getDocSize());
        item.put("status", history.getStatus());
        item.put("store", history.getStore());
        item.put("autoRun", history.isAutoRun());
        return item;
    }

    private Pageable getPagination(int from, int size){
        int page = 0;

        if (from > 0 ) page = (from / size);

        return PageRequest.of(page, size);
    }

    private String parseCollectionName(HistoryReadRequest historyReadRequest){
        if(historyReadRequest.getIndexA() != null ){
            return parseCollectionName(historyReadRequest.getIndexA());
        }else{
            return parseCollectionName(historyReadRequest.getIndexB());
        }
    }

    private String parseCollectionName(String index){
        int len = index.length();
        return  index.substring(0, len-2);
    }

}
