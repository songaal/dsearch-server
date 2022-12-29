package com.danawa.dsearch.server.jdbc.repository;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public interface JdbcRepository {
    SearchResponse findAll(UUID clusterId, String index) throws IOException;

    IndexResponse save(UUID clusterId, String index, Map<String, Object> source) throws IOException;
    UpdateResponse save(UUID clusterId, String index, String docId, Map<String, Object> source) throws IOException;

    DeleteResponse deleteById(UUID clusterId, String index, String docId) throws IOException;

    Map<String, Object> getDocuments(UUID clusterId, String index, StringBuffer sb);
}
