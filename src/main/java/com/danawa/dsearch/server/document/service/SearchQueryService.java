package com.danawa.dsearch.server.document.service;

import com.danawa.dsearch.server.document.dto.SearchQueryCreateRequest;
import com.danawa.dsearch.server.document.dto.SearchQueryUpdateRequest;
import com.danawa.dsearch.server.document.entity.SearchQuery;
import com.danawa.dsearch.server.document.repository.SearchQueryRepository;
import com.danawa.dsearch.server.indices.service.IndicesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Service
public class SearchQueryService {
    private static Logger logger = LoggerFactory.getLogger(SearchQueryService.class);

    private SearchQueryRepository searchQueryRepository;

    public SearchQueryService(SearchQueryRepository searchQueryRepository) {
        this.searchQueryRepository = searchQueryRepository;
    }

    public void initialize(UUID clusterId) throws IOException {
        searchQueryRepository.initialize(clusterId);
    }

    public List<SearchQuery> getSearchQueryList(UUID clusterId){
        return searchQueryRepository.findAll(clusterId);
    }

    public void deleteSearchQuery(UUID clusterId, String id){
        searchQueryRepository.delete(clusterId, id);
    }

    public SearchQuery createSearchQuery(UUID clusterId, SearchQueryCreateRequest request){
        SearchQuery searchQuery = makeSearchQuery(request);
        return searchQueryRepository.save(clusterId, searchQuery);
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
        return searchQueryRepository.update(clusterId, searchQuery);
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
