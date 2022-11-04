package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.service.history.repository.HistoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

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
        logger.info("History count: {}", select.size());
        return select.size();
    }
}
