package com.danawa.dsearch.server.collections.service.indexer;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;

import java.net.URISyntaxException;
import java.util.Map;

public interface IndexerClient {
    IndexerStatus getStatus(IndexingInfo indexingInfo);
    void deleteJob(IndexingInfo indexingInfo);
    String startJob(Map<String, Object> body, Collection collection) throws URISyntaxException;

    void stopJob(IndexingInfo status) throws URISyntaxException;

    void startGroupJob(IndexingInfo status, Collection collection, String groupSeq) throws URISyntaxException;
}
