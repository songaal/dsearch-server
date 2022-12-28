package com.danawa.dsearch.server.documentAnalysis.service;

import com.danawa.dsearch.server.documentAnalysis.adapter.SearchQueryAdapter;
import com.danawa.dsearch.server.documentAnalysis.dto.SearchQueryCreateRequest;
import com.danawa.dsearch.server.documentAnalysis.dto.SearchQueryUpdateRequest;
import com.danawa.dsearch.server.documentAnalysis.entity.SearchQuery;
import com.danawa.dsearch.server.documentAnalysis.repository.SearchQueryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Service
public class SearchQueryService {
    private static Logger logger = LoggerFactory.getLogger(SearchQueryService.class);


    private SearchQueryAdapter searchQueryAdapter;

    public SearchQueryService(SearchQueryAdapter searchQueryAdapter) {
        this.searchQueryAdapter = searchQueryAdapter;
    }

    public void initialize(UUID clusterId) throws IOException {
        searchQueryAdapter.initialize(clusterId);
    }

    public List<SearchQuery> getSearchQueryList(UUID clusterId){
        return searchQueryAdapter.findAll(clusterId);
    }

    public void deleteSearchQuery(UUID clusterId, String id){
        searchQueryAdapter.deleteById(clusterId, id);
    }

    public SearchQuery createSearchQuery(UUID clusterId, SearchQueryCreateRequest request){
        SearchQuery searchQuery = makeSearchQuery(request);
        return searchQueryAdapter.create(clusterId, searchQuery);
    }

    private SearchQuery makeSearchQuery( SearchQueryCreateRequest request){
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(request.getQuery());
        searchQuery.setName(request.getName());
        searchQuery.setIndex(request.getIndex());
        return searchQuery;
    }

    public SearchQuery updateSearchQuery(UUID clusterId, SearchQueryUpdateRequest request){
        SearchQuery searchQuery = makeSearchQuery(request);
        return searchQueryAdapter.update(clusterId, searchQuery);
    }

    private SearchQuery makeSearchQuery( SearchQueryUpdateRequest request){
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(request.getQuery());
        searchQuery.setName(request.getName());
        searchQuery.setIndex(request.getIndex());
        searchQuery.setId(request.getId());
        return searchQuery;
    }

}
