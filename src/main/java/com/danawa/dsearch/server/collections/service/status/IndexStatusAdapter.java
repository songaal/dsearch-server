package com.danawa.dsearch.server.collections.service.status;


import com.danawa.dsearch.server.collections.entity.IndexStatus;
import com.danawa.dsearch.server.collections.service.status.repository.StatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class IndexStatusAdapter {
    private static final Logger logger = LoggerFactory.getLogger(IndexStatusAdapter.class);
    private StatusRepository statusRepository;

    public IndexStatusAdapter(StatusRepository statusRepository){
        this.statusRepository = statusRepository;
    }

    public void create(IndexStatus indexStatus){
        IndexStatus entity = statusRepository.save(indexStatus);
        logger.info("{}", entity);
    }

    public void delete(UUID clusterId, String index, long startTime){
        statusRepository.deleteByClusterIdAndIndexAndStartTime(clusterId, index, startTime);
    }

    public List<IndexStatus> findAll(UUID clusterId, int size, int from){
        return statusRepository.findByClusterId(clusterId, PageRequest.of(from, size));
    }
}
