package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class PipelineService {
    private static Logger logger = LoggerFactory.getLogger(PipelineService.class);

    private final ElasticsearchFactory elasticsearchFactory;

    public PipelineService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public Response getPipeLineLists(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("GET", "_ingest/pipeline?pretty");
            Response response = restClient.performRequest(request);
            return response;
        }
    }

    public Response postPipeLine(UUID clusterId, String name, String body) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("POST", "_ingest/pipeline/" + name +"/_simulate");
            request.setJsonEntity(body);
            Response response = restClient.performRequest(request);
            return response;
        }
    }

    public Response postPipeLineDetail(UUID clusterId, String name, String body) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("POST", "_ingest/pipeline/" + name +"/_simulate?verbose");
            request.setJsonEntity(body);
            Response response = restClient.performRequest(request);
            return response;
        }
    }

    public Response setPipeLine(UUID clusterId, String name, String body) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("PUT", "_ingest/pipeline/" + name);
            request.setJsonEntity(body);
            Response response = restClient.performRequest(request);
            return response;
        }
    }

    public Response getPipeLine(UUID clusterId, String name) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("GET", "_ingest/pipeline/" + name);
            Response response = restClient.performRequest(request);
            return response;
        }
    }

    public Response deletePipeLine(UUID clusterId, String name) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("DELETE", "_ingest/pipeline/" + name);
            Response response = restClient.performRequest(request);
            return response;
        }
    }
}
