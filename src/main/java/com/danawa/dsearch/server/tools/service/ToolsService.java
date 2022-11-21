package com.danawa.dsearch.server.tools.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.tools.entity.AnalysisToolRequest;
import org.apache.http.util.EntityUtils;
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

    public String getPlugins(UUID clusterId) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("GET", "_cat/plugins");
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            return responseBody;
        }
    }

    public List<String> checkPlugins(UUID clusterId, Set<String> plugins) throws IOException{
        List<String> result = new ArrayList<>();
        for(String pluginName : plugins){
            if( isUsablePlugin(clusterId, pluginName) ){
                result.add(pluginName);
            }
        }
        return result;
    }

    private boolean isUsablePlugin(UUID clusterId, String pluginName) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            String method = "POST";
            String endPoint = "/_" + pluginName + "/analyze";

            /* 임의의 쿼리를 만들어서 보낸다 */
            String setJson = "{ \"index\": \"" + dictionaryIndex + "\", \n" +
                    "\"detail\": true, \n" +
                    "\"useForQuery\": true, \n" +
                    "\"text\": \"Sandisk Extream Z80 USB 16gb\"}";
            try{
                Request pluginRequest = new Request(method, endPoint);
                pluginRequest.setJsonEntity(setJson);
                Response pluginResponse = restClient.performRequest(pluginRequest);
                return true;
            }catch(ResponseException re){
                logger.trace("Plugin Check Fail: {}", re.getMessage());
                return false;
            }
        }
    }

    public String getDetailAnalysis(UUID clusterId, AnalysisToolRequest request) throws IOException {

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();

            String plugin = request.getPlugin();
            String text = request.getText();
            String useForQuery = Objects.isNull(request.getUseForQuery()) ? "false" : request.getUseForQuery();

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
            String response = EntityUtils.toString(pluginResponse.getEntity());
            return response;
        }
    }
}
