package com.danawa.dsearch.server.templates.adapter;

import org.elasticsearch.action.search.SearchResponse;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public interface IndexTemplateAdapter {

    String getTemplates(UUID clusterId) throws IOException;
    SearchResponse getTemplateCommentAll(UUID clusterId, String index) throws IOException;

    SearchResponse getTemplateCommentByName(UUID clusterId, String index, String name) throws IOException;

    void insertTemplateComment(UUID clusterId, String index, Map<String, Object> source) throws IOException;

    void updateTemplateComment(UUID clusterId, String index, String docId, Map<String, Object> source) throws IOException;
}
