package com.danawa.dsearch.server.jdbc.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.elasticsearch.action.DocWriteResponse;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.h2.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class JdbcRepositoryAdapter {
    private static Logger logger = LoggerFactory.getLogger(JdbcRepositoryAdapter.class);
    private ElasticsearchFactory elasticsearchFactory;

    private String jdbcIndex;

    public JdbcRepositoryAdapter(
            @Value("${dsearch.jdbc.setting}") String jdbcIndex,
            ElasticsearchFactory elasticsearchFactory){
        this.jdbcIndex = jdbcIndex;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public List<JdbcInfo> findAll(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(jdbcIndex);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(1000);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return convertSearchResponseToList(searchResponse);
        }
    }
    private List<JdbcInfo> convertSearchResponseToList(SearchResponse response){
        List<JdbcInfo> list = new ArrayList<>();

        for(SearchHit searchHit: response.getHits().getHits()){
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            sourceAsMap.put("_id", searchHit.getId());
            list.add(convertMapToJdbcInfo(sourceAsMap));
        }

        return list;
    }
    private JdbcInfo convertMapToJdbcInfo(Map<String, Object> map){
        JdbcInfo jdbcInfo = new JdbcInfo();
        if(map.get("_id") != null){
            jdbcInfo.set_id((String) map.get("_id"));
        }
        if(map.get("address") != null){
            jdbcInfo.setAddress((String) map.get("address"));
        }

        if(map.get("db_name") != null){
            jdbcInfo.setDb_name((String) map.get("db_name"));
        }

        if(map.get("driver") != null){
            jdbcInfo.setDriver((String) map.get("driver"));
        }

        if(map.get("id") != null){
            jdbcInfo.setId((String) map.get("id"));
        }

        if(map.get("name") != null){
            jdbcInfo.setName((String) map.get("name"));
        }

        if(map.get("password") != null){
            jdbcInfo.setPassword((String) map.get("password"));
        }

        if(map.get("provider") != null){
            jdbcInfo.setProvider((String) map.get("provider"));
        }

        if(map.get("url") != null){
            jdbcInfo.setUrl((String) map.get("url"));
        }

        if(map.get("user") != null){
            jdbcInfo.setUser((String) map.get("user"));
        }

        return jdbcInfo;
    }


    public boolean create(UUID clusterId, JdbcCreateRequest jdbcCreateRequest) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> jsonMap = new HashMap<>();

            covertCreateRequestToMap(jdbcCreateRequest, jsonMap);
            IndexRequest indexRequest = new IndexRequest(jdbcIndex).source(jsonMap);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

            return convertResponseToBoolean(indexResponse);
        }
    }

    private void covertCreateRequestToMap(JdbcCreateRequest jdbcRequest, Map<String, Object> jsonMap){
        String id = jdbcRequest.getId();
        String name = jdbcRequest.getName();
        String driver = jdbcRequest.getDriver();
        String user = jdbcRequest.getUser();
        String password = jdbcRequest.getPassword();
        String url = jdbcRequest.getUrl();
        String provider = jdbcRequest.getProvider();
        String address = jdbcRequest.getAddress();
        String port = jdbcRequest.getPort();
        String db_name = jdbcRequest.getDB_name();
        String params = jdbcRequest.getParams();

        if(!StringUtils.isNullOrEmpty(id)) jsonMap.put("id", id);
        if(!StringUtils.isNullOrEmpty(name)) jsonMap.put("name", name);
        if(!StringUtils.isNullOrEmpty(driver)) jsonMap.put("driver", driver);
        if(!StringUtils.isNullOrEmpty(user)) jsonMap.put("user", user);
        if(!StringUtils.isNullOrEmpty(password)) jsonMap.put("password", password);
        if(!StringUtils.isNullOrEmpty(url)) jsonMap.put("url", url);
        if(!StringUtils.isNullOrEmpty(provider)) jsonMap.put("provider", provider);
        if(!StringUtils.isNullOrEmpty(address)) jsonMap.put("address", address);
        if(!StringUtils.isNullOrEmpty(port)) jsonMap.put("port", port);
        if(!StringUtils.isNullOrEmpty(db_name)) jsonMap.put("db_name", db_name);
        if(!StringUtils.isNullOrEmpty(params)) jsonMap.put("params", params);
    }

    private boolean convertResponseToBoolean(DocWriteResponse response){
        switch (response.getResult()){
            case CREATED:
            case DELETED:
            case UPDATED:
                return true;
            default:
                return false;
        }
    }

    public boolean delete(UUID clusterId, String docId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            DeleteRequest request = new DeleteRequest(jdbcIndex, docId);
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            return convertResponseToBoolean(deleteResponse);
        }
    }

    public boolean update(UUID clusterId, String id, JdbcUpdateRequest jdbcRequest) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> jsonMap = new HashMap<>();

            covertUpdateRequestToMap(jdbcRequest, jsonMap);
            UpdateRequest updateRequest = new UpdateRequest(jdbcIndex, id).doc(jsonMap);
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            return convertResponseToBoolean(updateResponse);
        }
    }

    private void covertUpdateRequestToMap(JdbcUpdateRequest jdbcRequest, Map<String, Object> jsonMap){
        String id = jdbcRequest.getId();
        String name = jdbcRequest.getName();
        String driver = jdbcRequest.getDriver();
        String user = jdbcRequest.getUser();
        String password = jdbcRequest.getPassword();
        String url = jdbcRequest.getUrl();

        if(!StringUtils.isNullOrEmpty(id)) jsonMap.put("id", id);
        if(!StringUtils.isNullOrEmpty(name)) jsonMap.put("name", name);
        if(!StringUtils.isNullOrEmpty(driver)) jsonMap.put("driver", driver);
        if(!StringUtils.isNullOrEmpty(user)) jsonMap.put("user", user);
        if(!StringUtils.isNullOrEmpty(password)) jsonMap.put("password", password);
        if(!StringUtils.isNullOrEmpty(url)) jsonMap.put("url", url);
    }

    public void fillJdbcInfoList(UUID clusterId, StringBuffer sb, Map<String, Object> jdbc){
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(jdbcIndex).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10000).from(0));
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
                body.put("_index", jdbcIndex);
                body.put("_type", "_doc");
                body.put("_id", hit.getId());
                body.put("_score", hit.getScore());
                body.put("_source", hit.getSourceAsMap());
                list.add(hit.getSourceAsMap().get("id") + " [" + hit.getSourceAsMap().get("name") + "]");
                String stringBody = gson.toJson(body);
                sb.append(stringBody);
                count++;
            }
            jdbc.put("result", true);
            jdbc.put("count", hits.length);
            jdbc.put("list", list);
        } catch (IOException e) {
            jdbc.put("result", false);
            jdbc.put("count", 0);
            jdbc.put("message", e.getMessage());
            jdbc.put("list", new ArrayList<>());
            logger.error("{}", e);
        }
    }
}
