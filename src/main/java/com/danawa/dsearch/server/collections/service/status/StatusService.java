package com.danawa.dsearch.server.collections.service.status;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface StatusService {
    void initialize(UUID clusterId) throws IOException;
    void create(IndexingStatus status, String currentStatus) ;
    void delete(UUID clusterId, String index, long startTime) throws IOException, InterruptedException;
    void update(IndexingStatus indexingStatus, String status);

    List<Map<String, Object>> findAll(UUID clusterId, int size, int from) throws IOException;
}
