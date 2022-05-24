package com.danawa.dsearch.server;

import com.danawa.dsearch.server.clusters.entity.ClusterStatusResponse;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class EsTest {

    public void stringTest() {
        try {
            RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("127.0.0.1", 9200, "http"));
            RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

            Request request = new Request("GET", "/test-index/_nodes");
            request.addParameter("format", "json");
            request.addParameter("pretty", "json");
            request.setJsonEntity("{}");
            Response response = client.getLowLevelClient().performRequest(request);
        } catch (IOException e) {

        }
    }


    @Test
    public void state() {
        try {
            ClusterStatusResponse status = null;

            RestClientBuilder builder;
            RestHighLevelClient client = null;
            RestClient restClient = null;

            builder = RestClient.builder(new HttpHost("localhost", 9200));
            client = new RestHighLevelClient(builder);
            restClient = client.getLowLevelClient();

            Request nodesRequest = new Request("GET", "/_nodes");
            nodesRequest.addParameter("format", "json");
            nodesRequest.addParameter("human", "true");
            nodesRequest.addParameter("filter_path", "**.http.publish_address");
            String nodesResponse = convertResponseToString(restClient.performRequest(nodesRequest));
            Map<String, Object> nodes = new Gson().fromJson(nodesResponse, Map.class);

            Request stateRequest = new Request("GET", "/_stats");
            stateRequest.addParameter("format", "json");
            stateRequest.addParameter("human", "true");
            stateRequest.addParameter("filter_path", "_shards,_all.total.store,indices.*.uuid");
            String stateResponse = convertResponseToString(restClient.performRequest(stateRequest));
            Map<String, Object> state = new Gson().fromJson(stateResponse, Map.class);

            System.out.println(stateResponse);

            status = new ClusterStatusResponse(true, nodes, state);
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    static String convertResponseToString(Response response) {
        String responseBody = null;
        try {
            responseBody = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {

        }
        return responseBody;
    }

}
