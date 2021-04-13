package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.excpetions.DuplicateException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class IndexTemplateService {

    private ElasticsearchFactory factory;
    private String commentsIndex;
    private IndicesService indicesService;
    private final String COMMENTS_JSON = "comments.json";

    public IndexTemplateService(@Value("${dsearch.comments.setting}") String commentsIndex, IndicesService indicesService, ElasticsearchFactory factory){
        this.commentsIndex = commentsIndex;
        this.factory = factory;
        this.indicesService = indicesService;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, commentsIndex, COMMENTS_JSON);
    }

    public SearchResponse getTemplateComment(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = factory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(commentsIndex);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(10000);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }





    public void setTemplateComment(UUID clusterId, Map<String, Object> comments) throws IOException {
        try (RestHighLevelClient client = factory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(commentsIndex);
            SearchSourceBuilder builder = new SearchSourceBuilder();
            builder.query(QueryBuilders.matchQuery("name", comments.get("name")));
            searchRequest.source(builder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            if(searchResponse.getHits().getHits().length == 0){
                Map<String, Object> data = (Map) comments.get("data");
                XContentBuilder source = jsonBuilder()
                        .startObject()
                        .field("comments", data.get("comments"))
                        .field("name", comments.get("name"))
                        .endObject();

                IndexRequest indexRequest = new IndexRequest();
                indexRequest.index(commentsIndex);
                indexRequest.source(source);
                client.index(indexRequest, RequestOptions.DEFAULT).toString();
            }else if(comments.get("id") == null){
                SearchHit[] hits = searchResponse.getHits().getHits();

                Map<String, Object> map = new HashMap<>();
                Map<String, Object> data = (Map) comments.get("data");
                map.put("comments", data.get("comments"));
                map.put("name", data.get("name"));

                client.update(new UpdateRequest()
                        .index(commentsIndex)
                        .id(hits[0].getId())
                        .doc(map), RequestOptions.DEFAULT);
            }else{
                // 설명이 있을때
                Map<String, Object> map = new HashMap<>();
                Map<String, Object> data = (Map) comments.get("data");
                map.put("comments", data.get("comments"));
                map.put("name", data.get("name"));

                client.update(new UpdateRequest()
                        .index(commentsIndex)
                        .id((String) comments.get("id"))
                        .doc(map), RequestOptions.DEFAULT);
            }
        }
    }
}
