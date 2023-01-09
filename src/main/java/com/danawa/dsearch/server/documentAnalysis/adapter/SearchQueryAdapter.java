package com.danawa.dsearch.server.documentAnalysis.adapter;

import com.danawa.dsearch.server.documentAnalysis.entity.SearchQuery;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public interface SearchQueryAdapter {
    void initialize(UUID clusterId) throws IOException;
    List<SearchQuery> findAll(UUID clusterId);
    void deleteById(UUID clusterId, String id);
    SearchQuery create(UUID clusterId, SearchQuery searchQuery);
    SearchQuery update(UUID clusterId, SearchQuery searchQuery);
}
