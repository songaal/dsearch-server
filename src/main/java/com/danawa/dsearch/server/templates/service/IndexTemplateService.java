package com.danawa.dsearch.server.templates.service;

import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.templates.adapter.IndexTemplateAdapter;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
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

    private String commentsIndex;
    private IndicesService indicesService;
    private IndexTemplateAdapter indexTemplateAdapter;
    private final String COMMENTS_JSON = "comments.json";
    public IndexTemplateService(@Value("${dsearch.comments.setting}") String commentsIndex,
                                IndicesService indicesService,
                                IndexTemplateAdapter indexTemplateAdapter){
        this.indexTemplateAdapter = indexTemplateAdapter;
        this.commentsIndex = commentsIndex;
        this.indicesService = indicesService;
    }

    public void initialize(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, commentsIndex, COMMENTS_JSON);
    }

    public SearchResponse getTemplateComment(UUID clusterId) throws IOException {
        return indexTemplateAdapter.getTemplateCommentAll(clusterId, commentsIndex);
    }


    public void saveTemplateComment(UUID clusterId, Map<String, Object> comments) throws IOException {
        String commentsName = (String) comments.get("name");
        SearchResponse searchResponse = indexTemplateAdapter.getTemplateCommentByName(clusterId, commentsIndex, commentsName);

        Map<String, Object> data = (Map) comments.get("data");

        Map<String, Object> map = new HashMap<>();
        map.put("comments", data.get("comments"));
        map.put("name", data.get("name"));

        if (searchResponse.getHits().getHits().length == 0) {
            Map<String, Object> source = new HashMap<>();
            source.put("comments", data.get("comments"));
            source.put("name", comments.get("name"));
            indexTemplateAdapter.insertTemplateComment(clusterId, commentsIndex, source);
        } else if (comments.get("id") == null) {
            SearchHit[] hits = searchResponse.getHits().getHits();
            String docId = hits[0].getId();
            indexTemplateAdapter.updateTemplateComment(clusterId, commentsIndex, docId, map);
        } else {
            String docId = (String) comments.get("id");
            indexTemplateAdapter.updateTemplateComment(clusterId, commentsIndex, docId, map);
        }
    }


    public String download(UUID clusterId, Map<String, Object> message) throws IOException{
        List<String> list = new ArrayList<>();

        String templates = indexTemplateAdapter.getTemplates(clusterId);

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
        List<String> commentsNameList = new ArrayList<>();

        try{
            SearchResponse response = indexTemplateAdapter.getTemplateCommentAll(clusterId, commentsIndex);
            SearchHit[] hits = response.getHits().getHits();

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
                commentsNameList.add(hit.getSourceAsMap().get("name") + "");
                String stringBody = gson.toJson(body);
                sb.append(stringBody);
                count++;
            }

            comments.put("result", true);
            comments.put("count", hits.length);
            comments.put("list", commentsNameList);
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
