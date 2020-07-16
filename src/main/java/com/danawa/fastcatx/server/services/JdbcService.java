package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.JdbcRequest;
import com.danawa.fastcatx.server.entity.JdbcDeleteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

    public JdbcService(@Value("${fastcatx.jdbc.setting}") String jdbcIndex, IndicesService indicesService, ElasticsearchFactory elasticsearchFactory) {
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

            String query = "{\n" +
                    "        \"size\": 10000\n" +
                    "      }";
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }

    public IndexResponse addJdbcSource(UUID clusterId, JdbcRequest jdbcRequest) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> jsonMap = new HashMap<>();

            jsonMap.put("id", jdbcRequest.getId());
            jsonMap.put("name", jdbcRequest.getName());
            jsonMap.put("provider", jdbcRequest.getProvider());
            jsonMap.put("driver", jdbcRequest.getDriver());
            jsonMap.put("address", jdbcRequest.getAddress());
            jsonMap.put("port", jdbcRequest.getPort());
            jsonMap.put("db_name", jdbcRequest.getDB_name());
            jsonMap.put("user", jdbcRequest.getUser());
            jsonMap.put("password", jdbcRequest.getPassword());
            jsonMap.put("params", jdbcRequest.getParams());
            jsonMap.put("url", jdbcRequest.getUrl());

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

            if(jdbcRequest.getId() != null && !jdbcRequest.getId().equals("")) jsonMap.put("id", jdbcRequest.getId());
            if(jdbcRequest.getName() != null && !jdbcRequest.getName().equals("")) jsonMap.put("name", jdbcRequest.getName());
            if(jdbcRequest.getDriver() != null && !jdbcRequest.getDriver().equals("")) jsonMap.put("driver", jdbcRequest.getDriver());
            if(jdbcRequest.getUser() != null && !jdbcRequest.getUser().equals("")) jsonMap.put("user", jdbcRequest.getUser());
            if(jdbcRequest.getPassword() != null && !jdbcRequest.getPassword().equals("")) jsonMap.put("password", jdbcRequest.getPassword());
            if(jdbcRequest.getUrl() != null && !jdbcRequest.getUrl().equals("")) jsonMap.put("url", jdbcRequest.getUrl());

            UpdateRequest updateRequest = new UpdateRequest(jdbcIndex, id).doc(jsonMap);
            UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            return updateResponse;
        }
    }
}
