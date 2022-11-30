package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface HistoryService {
    void initialize(UUID clusterId) throws IOException;
    void create(IndexingInfo indexingInfo, String status);
    void create(IndexingInfo indexingInfo, String docSize, String status, String store);
    void delete(UUID clusterId, String collectionId);
    List<Map<String,Object>> findByCollection(UUID clusterId, HistoryReadRequest historyReadRequest);
    long getTotalSize(UUID clusterId, HistoryReadRequest historyReadRequest);
}
