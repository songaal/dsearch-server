package com.danawa.dsearch.server.jdbc.repository;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.hibernate.sql.Update;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Service
public class JdbcEsRepository implements JdbcRepository{

    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;
    JdbcEsRepository(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    @Override
    public SearchResponse findAll(UUID clusterId, String index) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(1000);
        searchRequest.source(searchSourceBuilder);
        return elasticsearchFactoryHighLevelWrapper.search(clusterId, searchRequest);
    }

    /**
     * Insert 전용
     * @param clusterId
     * @param index
     * @param source
     * @return
     * @throws IOException
     */
    @Override
    public IndexResponse save(UUID clusterId, String index, Map<String, Object> source) throws IOException {
        return  elasticsearchFactoryHighLevelWrapper.insertDocument(clusterId, index, source);
    }


    /**
     * update 전용
     * @param clusterId
     * @param index
     * @param docId
     * @param source
     * @return
     * @throws IOException
     */
    @Override
    public UpdateResponse save(UUID clusterId, String index, String docId, Map<String, Object> source) throws IOException {
        return  elasticsearchFactoryHighLevelWrapper.updateDocument(clusterId, index, docId, source);
    }

    @Override
    public DeleteResponse deleteById(UUID clusterId, String index, String docId) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.deleteDocument(clusterId, index, docId);
    }

    @Override
    public Map<String, Object> getDocuments(UUID clusterId, String index, StringBuffer sb){
        return elasticsearchFactoryHighLevelWrapper.getDocuments(clusterId, index, sb);
    }
}
