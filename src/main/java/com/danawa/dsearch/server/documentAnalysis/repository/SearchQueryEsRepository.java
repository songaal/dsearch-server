package com.danawa.dsearch.server.documentAnalysis.repository;

import com.danawa.dsearch.server.documentAnalysis.entity.SearchQuery;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

@Component
public class SearchQueryEsRepository implements SearchQueryRepository {

    private static Logger logger = LoggerFactory.getLogger(SearchQueryEsRepository.class);

    private String index = ".dsearch_analysis_query_index";

    private final String ANALYSIS_QUERY_INDEX_JSON = "analysis_query_index.json";

    private final ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    public SearchQueryEsRepository(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
        elasticsearchFactoryHighLevelWrapper.createIndex(clusterId, index, ANALYSIS_QUERY_INDEX_JSON);
    }

    @Override
    public List<SearchQuery> findAll(UUID clusterId) {
        try{
            // 1. query 만들기
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.sort("name", SortOrder.DESC);
            searchSourceBuilder.size(10000);

            // 검색
            List<Map<String, Object>> searchList = elasticsearchFactoryHighLevelWrapper.search(clusterId, index, searchSourceBuilder.toString());

            // 3. 리스트 반환
            return convertResponseToList(searchList);
        } catch (IOException e) {
            logger.error("{}", e);
            return new ArrayList<>();
        }
    }

    private List<SearchQuery> convertResponseToList(List<Map<String, Object>> searchList){
        if (searchList.size() == 0)
            return new ArrayList<>();

        List<SearchQuery> result = new ArrayList<>();
        for(Map<String, Object> item : searchList){
            String docId = (String) item.get("_id");
            Map<String, Object> source = (Map<String, Object>) item.get("_source");
            SearchQuery searchQuery = convertMapToSearchQuery(docId, source);
            result.add(searchQuery);
        }
        return result;
    }

    private SearchQuery convertMapToSearchQuery(String docId, Map<String, Object> item){
        SearchQuery searchQuery = new SearchQuery();
        String index = (String) item.get("index");
        String query = (String) item.get("query");
        String name = (String) item.get("name");
        searchQuery.setId(docId);
        searchQuery.setIndex(index);
        searchQuery.setQuery(query);
        searchQuery.setName(name);
        return searchQuery;
    }

    @Override
    public void delete(UUID clusterId, String id) {
        try{
            String result = elasticsearchFactoryHighLevelWrapper.deleteIndex(clusterId, index, id);
            logger.info("SearchQuery Deleted !!! >> clusterId={}, index={}, id={}, result={}", clusterId, index, id, result);
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    @Override
    public SearchQuery save(UUID clusterId, SearchQuery searchQuery) {
        try{
            Map<String, Object> source = convertSearchQueryToMap(searchQuery);
            IndexResponse response = elasticsearchFactoryHighLevelWrapper.insertDocument(clusterId, index, source);
            searchQuery.setId(response.getId());
        } catch (IOException e) {
            logger.error("{}", e);
        }

        return searchQuery;
    }

    private Map<String, Object> convertSearchQueryToMap(SearchQuery searchQuery){
        Map<String, Object> result = new HashMap<>();
        result.put("index", searchQuery.getIndex());
        result.put("query", searchQuery.getQuery());
        result.put("name", searchQuery.getName());
        return result;
    }


    @Override
    public SearchQuery update(UUID clusterId, SearchQuery searchQuery) {

        try{
            Map<String, Object> source = convertSearchQueryToMap(searchQuery);
            UpdateResponse updateResponse = elasticsearchFactoryHighLevelWrapper.updateDocument(clusterId, index, source);
        } catch (IOException e) {
            logger.error("{}", e);
        }

        return searchQuery;
    }


}
