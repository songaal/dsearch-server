package com.danawa.dsearch.server.indices.adapter;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.indices.AnalyzeResponse;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface IndicesAdapter {

    void createIndex(UUID clusterId, String index, String source) throws IOException;
    void deleteIndex(UUID clusterId, String index) throws IOException;

    SearchResponse getDocuments(UUID clusterId, SearchRequest searchRequest) throws IOException;

    Map<String, Object> getFieldsMappings(UUID clusterId, String index) throws IOException;

    List<AnalyzeResponse.AnalyzeToken> analyze(UUID clusterId, String index, String analyzer, String text) throws IOException;

    SearchResponse findAll(UUID clusterId, String index) throws IOException;
}
