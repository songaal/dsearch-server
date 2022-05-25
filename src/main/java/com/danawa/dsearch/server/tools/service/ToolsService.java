package com.danawa.dsearch.server.tools.service;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class ToolsService {
    private static Logger logger = LoggerFactory.getLogger(ToolsService.class);
    private final ElasticsearchFactory elasticsearchFactory;
    private String dictionaryIndex;

    public ToolsService(@Value("${dsearch.dictionary.index}") String dictionaryIndex, ElasticsearchFactory elasticsearchFactory) {
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
                    logger.trace("Plugin Check Fail: {}", re.getMessage());
                }
            }
            return result;
        }
    }

    public Response getDetailAnalysis(UUID clusterId, Map<String, Object> request) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();

            String plugin = (String) request.get("plugin");
            String text = (String) request.get("text");
            String useForQuery = Objects.isNull(request.get("useForQuery")) ? "false" : String.valueOf(request.get("useForQuery"));

            String method = "POST";
            String endPoint = "/_" + plugin + "/analyze";
            String setJson = "{ \"index\": \"" + dictionaryIndex + "\", \n" +
                    "\"detail\": true, \n" +
                    "\"useForQuery\": " + useForQuery + ", \n" +
                    "\"text\": \"" + text + "\"}";

            logger.info("{}", setJson);
            Request pluginRequest = new Request(method, endPoint);
            pluginRequest.setJsonEntity(setJson);
            Response pluginResponse = restClient.performRequest(pluginRequest);

            return pluginResponse;
        }
    }
}
