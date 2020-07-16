package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.DetailAnalysisRequest;
import com.danawa.fastcatx.server.entity.JdbcRequest;
import com.danawa.fastcatx.server.entity.RankingTuningRequest;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
public class ToolsService {
    private static Logger logger = LoggerFactory.getLogger(ToolsService.class);

    private final ElasticsearchFactory elasticsearchFactory;
    private String dictionaryIndex;

    public ToolsService(@Value("${fastcatx.dictionary.index}") String dictionaryIndex, ElasticsearchFactory elasticsearchFactory) {
        this.dictionaryIndex = dictionaryIndex;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public Response getPlugins(UUID clusterId) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("GET", "_cat/plugins");
            Response response = restClient.performRequest(request);
            return response;
        }
    }

    public List<String> checkPlugins(UUID clusterId, Set<String> plugins) throws IOException{
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();

            List<String> result = new ArrayList<>();
            for(String plugin : plugins){
                String method = "POST";
                String endPoint = "/_" + plugin + "/analyze";
                /* 임의의 쿼리를 만들어서 보낸다 */
                String setJson = "{ \"index\": \"" + dictionaryIndex + "\", \n" +
                        "\"detail\": true, \n" +
                        "\"useForQuery\": true, \n" +
                        "\"text\": \"Sandisk Extream Z80 USB 16gb\"}";
                try{
                    Request pluginRequest = new Request(method, endPoint);
                    pluginRequest.setJsonEntity(setJson);
                    Response pluginResponse = restClient.performRequest(pluginRequest);
                    result.add(plugin);
                }catch(ResponseException re){
                    logger.error("", re);
                }
            }
            return result;
        }
    }

    public Response getDetailAnalysis(UUID clusterId, DetailAnalysisRequest detailAnalysisRequest) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            String plugin = detailAnalysisRequest.getPlugin();
            String text = detailAnalysisRequest.getText();

            String method = "POST";
            String endPoint = "/_" + plugin + "/analyze";
            String setJson = "{ \"index\": \"" + dictionaryIndex + "\", \n" +
                    "\"detail\": true, \n" +
                    "\"useForQuery\": true, \n" +
                    "\"text\": \"" + text + "\"}";

            Request pluginRequest = new Request(method, endPoint);
            pluginRequest.setJsonEntity(setJson);
            Response pluginResponse = restClient.performRequest(pluginRequest);
            return pluginResponse;
        }
    }
}
