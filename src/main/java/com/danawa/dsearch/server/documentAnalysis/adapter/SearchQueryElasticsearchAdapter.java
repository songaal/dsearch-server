package com.danawa.dsearch.server.documentAnalysis.adapter;

import com.danawa.dsearch.server.documentAnalysis.dto.SearchQueryCreateRequest;
import com.danawa.dsearch.server.documentAnalysis.dto.SearchQueryUpdateRequest;
import com.danawa.dsearch.server.documentAnalysis.entity.SearchQuery;
import com.danawa.dsearch.server.documentAnalysis.repository.SearchQueryRepository;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Service
public class SearchQueryElasticsearchAdapter implements SearchQueryAdapter{

    private SearchQueryRepository searchQueryRepository;

    SearchQueryElasticsearchAdapter(SearchQueryRepository searchQueryRepository){
        this.searchQueryRepository = searchQueryRepository;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
        searchQueryRepository.initialize(clusterId);
    }

    @Override
    public List<SearchQuery> findAll(UUID clusterId) {
        return searchQueryRepository.findAll(clusterId);
    }

    @Override
    public void deleteById(UUID clusterId, String id) {
        searchQueryRepository.delete(clusterId, id);
    }

    @Override
    public SearchQuery create(UUID clusterId, SearchQuery searchQuery) {
        return searchQueryRepository.save(clusterId, searchQuery);
    }

    @Override
    public SearchQuery update(UUID clusterId, SearchQuery searchQuery) {
        return searchQueryRepository.update(clusterId, searchQuery);
    }
}
