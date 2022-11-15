package com.danawa.dsearch.server.collections.service.indexer;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;

import java.net.URISyntaxException;
import java.util.Map;

public interface IndexerClient {
    IndexerStatus getStatus(IndexingStatus indexingStatus);
    void deleteJob(IndexingStatus indexingStatus);
    String startJob(Map<String, Object> body, Collection collection) throws URISyntaxException;

    void stopJob(IndexingStatus status) throws URISyntaxException;

    void startGroupJob(IndexingStatus status, Collection collection, String groupSeq) throws URISyntaxException;
}
