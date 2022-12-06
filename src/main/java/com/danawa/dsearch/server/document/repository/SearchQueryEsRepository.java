package com.danawa.dsearch.server.document.repository;

import com.danawa.dsearch.server.document.entity.SearchQuery;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.CreateRepositoryException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

@Component
public class SearchQueryEsRepository implements SearchQueryRepository {

    private static Logger logger = LoggerFactory.getLogger(SearchQueryEsRepository.class);

    private String index = ".dsearch_analysis_query_index";

    private final String ANALYSIS_QUERY_INDEX_JSON = "analysis_query_index.json";

    private final ElasticsearchFactory elasticsearchFactory;

    public SearchQueryEsRepository(ElasticsearchFactory elasticsearchFactory){
        this.elasticsearchFactory = elasticsearchFactory;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            if (!client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                client.indices().create(new CreateIndexRequest(index)
                                .source(StreamUtils.copyToString(new ClassPathResource(ANALYSIS_QUERY_INDEX_JSON).getInputStream(),
                                        Charset.defaultCharset()), XContentType.JSON),
                        RequestOptions.DEFAULT);
            }
        }
    }

    @Override
    public List<SearchQuery> findAll(UUID clusterId) {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            // 1. SearchRequest 만들기
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.sort("name", SortOrder.DESC);
            searchRequest.source(searchSourceBuilder);

            // 2. 결과 조회
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            // 3. 리스트 반환
            return convertResponseToList(response);
        } catch (IOException e) {
            logger.error("{}", e);
            return new ArrayList<>();
        }
    }

    private List<SearchQuery> convertResponseToList(SearchResponse searchResponse){
        if (searchResponse.getHits().getTotalHits().value == 0)
            return new ArrayList<>();

        List<SearchQuery> result = new ArrayList<>();
        for(SearchHit item : searchResponse.getHits().getHits()){
            Map<String, Object> itemMap = item.getSourceAsMap();
            SearchQuery searchQuery = convertMapToSearchQuery(item.getId(), itemMap);
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
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            // 1. request 만들기
            DeleteRequest deleteRequest = new DeleteRequest(index);
            deleteRequest.id(id);

            // 2. 삭제
            DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
            logger.info("SearchQuery Deleted !!! >> clusterId={}, index={}, id={}, result={}", clusterId, index, id, response.getResult());
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    @Override
    public SearchQuery save(UUID clusterId, SearchQuery searchQuery) {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            // 1. request 만들기
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index(index).source(convertSearchQueryToMap(searchQuery), XContentType.JSON);

            // 2. 결과 조회
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
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
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            // 1. request 만들기
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index);
            updateRequest.doc(convertSearchQueryToMap(searchQuery), XContentType.JSON);

            // 2. 결과 조회
            UpdateResponse response = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("{}", e);
        }
        // 3. 실패 했을 경우 ?

        return searchQuery;
    }


}
