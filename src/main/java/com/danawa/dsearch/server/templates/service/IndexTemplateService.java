package com.danawa.dsearch.server.templates.service;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class IndexTemplateService {
    private static Logger logger = LoggerFactory.getLogger(IndexTemplateService.class);

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


    public String download(UUID clusterId, Map<String, Object> message) throws IOException{
        String templates = "{}";
        try (RestHighLevelClient client = factory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("GET", "_template");
//            request.addParameter("format", "json");
            Response response = restClient.performRequest(request);
            templates = EntityUtils.toString(response.getEntity());
        }

        List<String> list = new ArrayList<>();

        Gson gson = JsonUtils.createCustomGson();
        Map<String, Object> templateAllMap = gson.fromJson(templates, Map.class);
        Map<String, Object> templateMap = new HashMap<>();
        for(String key: templateAllMap.keySet()){
            if(!key.startsWith(".")){
                templateMap.put(key, templateAllMap.get(key));
                list.add(key);
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put("result", true);
        result.put("count", templateMap.size());
        result.put("list", list);
        message.put("templates", result);
        return gson.toJson(templateMap);
    }

    public String commentDownload(UUID clusterId, Map<String, Object> message) {
        StringBuffer sb = new StringBuffer();
        Map<String, Object> comments = new HashMap<>();
        try (RestHighLevelClient client = factory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(commentsIndex).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10000).from(0));
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();

            List<String> list = new ArrayList<>();

            Gson gson = JsonUtils.createCustomGson();
            int count = 0;
            for(SearchHit hit : hits){
                if(count != 0){
                    sb.append(",\n");
                }
                Map<String, Object> body = new HashMap<>();
                body.put("_index", commentsIndex);
                body.put("_type", "_doc");
                body.put("_id", hit.getId());
                body.put("_score", hit.getScore());
                body.put("_source", hit.getSourceAsMap());
                list.add(hit.getSourceAsMap().get("name") + "");
                String stringBody = gson.toJson(body);
                sb.append(stringBody);
                count++;
            }

            comments.put("result", true);
            comments.put("count", hits.length);
            comments.put("list", list);
        } catch (IOException e) {
            comments.put("result", false);
            comments.put("count", 0);
            comments.put("message", e.getMessage());
            comments.put("list", new ArrayList<>());
            logger.error("{}", e);
        }

        message.put("comments", comments);
        return sb.toString();
    }


}
