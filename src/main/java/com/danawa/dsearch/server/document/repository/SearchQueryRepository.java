package com.danawa.dsearch.server.document.repository;

import com.danawa.dsearch.server.document.entity.SearchQuery;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public interface SearchQueryRepository {

    void initialize(UUID clusterId) throws IOException;

    List<SearchQuery> findAll(UUID clusterId);

    void delete(UUID clusterId, String id);

    SearchQuery save(UUID clusterId, SearchQuery searchQuery);

    SearchQuery update(UUID clusterId, SearchQuery searchQuery);
}
