package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface HistoryService {
    void initialize(UUID clusterId) throws IOException;
    void create(IndexingStatus indexingStatus, String status);
    void create(IndexingStatus indexingStatus, String docSize, String status, String store);
    void delete(UUID clusterId, String collectionId);

    List<Map<String,Object>> findByCollection(UUID clusterId, HistoryReadRequest historyReadRequest);
    long getTotalSize(UUID clusterId, HistoryReadRequest historyReadRequest);
}
