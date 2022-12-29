package com.danawa.dsearch.server.templates.adapter;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@Service
public class IndexTemplateEsAdapter implements IndexTemplateAdapter{

    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    IndexTemplateEsAdapter(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    @Override
    public String getTemplates(UUID clusterId) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.getTemplates(clusterId);
    }

    @Override
    public SearchResponse getTemplateCommentAll(UUID clusterId, String index) throws IOException {
        SearchRequest searchRequest = makeTemplateSearchRequest(index);
        return elasticsearchFactoryHighLevelWrapper.search(clusterId, searchRequest);
    }

    @Override
    public SearchResponse getTemplateCommentByName(UUID clusterId, String index, String name) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchQuery("name", name));
        searchRequest.source(builder);
        return elasticsearchFactoryHighLevelWrapper.search(clusterId, searchRequest);
    }

    @Override
    public void insertTemplateComment(UUID clusterId, String index, Map<String, Object> source) throws IOException {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index);
        indexRequest.source(source);
        elasticsearchFactoryHighLevelWrapper.insertDocument(clusterId, indexRequest);
    }

    @Override
    public void updateTemplateComment(UUID clusterId, String index, String docId, Map<String, Object> source) throws IOException {
        elasticsearchFactoryHighLevelWrapper.updateDocument(clusterId, index, docId, source);
    }

    private SearchRequest makeTemplateSearchRequest(String index){
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(10000);
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

}
