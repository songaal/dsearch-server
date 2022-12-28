package com.danawa.dsearch.server.documentAnalysis.adapter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface DocumentAnalysisAdapter {
    Map<String, Object> getIndexMappings(UUID clusterID, String index) throws IOException;

    List<Map<String, Object>> findAll(UUID clusterId, String index, String mergedQuery) throws IOException;

    Map<String, Object> getTerms(UUID clusterId, String index, String docId, String[] fields) throws IOException;

    Map<String, Object> findById(UUID clusterId, String index, String docId) throws IOException;
}
