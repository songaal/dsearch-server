package com.danawa.dsearch.server.collections.service.status;


import com.danawa.dsearch.server.collections.entity.IndexStatus;
import com.danawa.dsearch.server.collections.service.status.repository.StatusRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
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
        try{
            IndexStatus entity = statusRepository.save(indexStatus);
            logger.info("{} {} {}", entity.getClusterId(), entity.getIndex(), entity.getId());
        }catch (Exception e){
            logger.info("", e);
        }
    }

    @Transactional
    public void delete(UUID clusterId, String index, long startTime){
        try{
            statusRepository.deleteByClusterIdAndIndexAndStartTime(clusterId, index, startTime);
        }catch (Exception e){
            logger.info("", e);
        }

    }

    public List<IndexStatus> findAll(UUID clusterId, int size, int from){
        try{
            return statusRepository.findByClusterId(clusterId, PageRequest.of(from, size));
        }catch (Exception e){
            logger.info("", e);
        }

        return new ArrayList<>();
    }
}
