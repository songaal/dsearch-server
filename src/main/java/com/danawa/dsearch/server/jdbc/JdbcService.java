package com.danawa.dsearch.server.jdbc;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.IndicesService;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.h2.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;


@Service
public class JdbcService {
    private static Logger logger = LoggerFactory.getLogger(JdbcService.class);
    private String jdbcIndex;
    private final String JDBC_JSON = "jdbc.json";
    private final ElasticsearchFactory elasticsearchFactory;
    private IndicesService indicesService;

    public JdbcService(@Value("${dsearch.jdbc.setting}") String jdbcIndex, IndicesService indicesService, ElasticsearchFactory elasticsearchFactory) {
        this.jdbcIndex = jdbcIndex;
        this.indicesService = indicesService;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, jdbcIndex, JDBC_JSON);
    }

    public boolean connectionTest(JdbcRequest jdbcRequest){
        boolean flag = false;

        try{
            String url = jdbcRequest.getUrl();
            Class.forName(jdbcRequest.getDriver());
            Connection connection = null;
            connection = DriverManager.getConnection(url, jdbcRequest.getUser(), jdbcRequest.getPassword());
            connection.close();
            flag = true;
        }catch (SQLException sqlException){
            logger.debug("", sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            logger.debug("", classNotFoundException);
        } catch (Exception e){
            logger.debug("", e);
        }
        return flag;
    }

    public SearchResponse getJdbcList(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(jdbcIndex);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()).size(1000);
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }

    public IndexResponse addJdbcSource(UUID clusterId, JdbcRequest jdbcRequest) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> jsonMap = new HashMap<>();

            handleErrorJdbcSource(jdbcRequest, jsonMap, JdbcRequestType.ADD);
            IndexRequest indexRequest = new IndexRequest(jdbcIndex).source(jsonMap);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            return indexResponse;
        }
    }

    public DeleteResponse deleteJdbcSource(UUID clusterId, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            DeleteRequest request = new DeleteRequest(jdbcIndex, id);
            DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
            return deleteResponse;
        }
    }

    public UpdateResponse updateJdbcSource(UUID clusterId, String id, JdbcRequest jdbcRequest) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> jsonMap = new HashMap<>();

            handleErrorJdbcSource(jdbcRequest, jsonMap, JdbcRequestType.UPDATE);
            UpdateRequest updateRequest = new UpdateRequest(jdbcIndex, id).doc(jsonMap);
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            return updateResponse;
        }
    }

    public String download(UUID clusterId, Map<String, Object> message){
        StringBuffer sb = new StringBuffer();
        Map<String, Object> jdbc = new HashMap<>();
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
        message.put("jdbc", jdbc);
        return sb.toString();
    }

    enum JdbcRequestType{
        ADD, UPDATE
    }

    private void handleErrorJdbcSource(JdbcRequest jdbcRequest, Map<String, Object> jsonMap, JdbcRequestType type){

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

        switch (type){
            case ADD:
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
                break;
            case UPDATE:
                if(!StringUtils.isNullOrEmpty(id)) jsonMap.put("id", id);
                if(!StringUtils.isNullOrEmpty(name)) jsonMap.put("name", name);
                if(!StringUtils.isNullOrEmpty(driver)) jsonMap.put("driver", driver);
                if(!StringUtils.isNullOrEmpty(user)) jsonMap.put("user", user);
                if(!StringUtils.isNullOrEmpty(password)) jsonMap.put("password", password);
                if(!StringUtils.isNullOrEmpty(url)) jsonMap.put("url", url);
                break;
            default:
        }
    }
}
