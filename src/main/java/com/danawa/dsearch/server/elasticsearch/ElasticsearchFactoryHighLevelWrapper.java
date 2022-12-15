package com.danawa.dsearch.server.elasticsearch;

import com.danawa.dsearch.server.utils.JsonUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class ElasticsearchFactoryHighLevelWrapper {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFactoryHighLevelWrapper.class);

    private final ElasticsearchFactory elasticsearchFactory;

    public ElasticsearchFactoryHighLevelWrapper(ElasticsearchFactory elasticsearchFactory){
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public String getAliases(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient lowLevelClient = client.getLowLevelClient();
            Request request = new Request("GET", "_cat/aliases");
            request.addParameter("format", "json");
            Response response = lowLevelClient.performRequest(request);
            return EntityUtils.toString(response.getEntity());
        }
    }

    public boolean createIndexSettings(UUID clusterId, String index, Map<String, Object> settings) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            return client.indices().create(new CreateIndexRequest(index).settings(settings), RequestOptions.DEFAULT).isAcknowledged();
        }
    }

    public boolean updateIndexSettings(UUID clusterId, String index, Map<String, Object> settings) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
             return client.indices().putSettings(new UpdateSettingsRequest().indices(index).settings(settings), RequestOptions.DEFAULT).isAcknowledged();
        }
    }

    public Map<String, Object> getIndexDocument(UUID clusterId, String index, String docId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(
                    new GetRequest().index(index).id(docId), RequestOptions.DEFAULT);
            return getResponse.getSourceAsMap();
        }
    }

    public void updateAliases(UUID clusterId, IndicesAliasesRequest request) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            client.indices().updateAliases(request, RequestOptions.DEFAULT);
        }
    }

    public SearchHit[] search(UUID clusterId, SearchRequest request) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            return searchResponse.getHits().getHits();
        }
    }

    public boolean isExistIndex(UUID clusterId, String index) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            GetIndexRequest getIndexRequest = new GetIndexRequest(index);
            return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        }
    }

    public List getClusterShardSettings(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", "/_cat/shards");
            request.addParameter("h", "index,state");
            request.addParameter("format", "json");
            Response response = client.getLowLevelClient().performRequest(request);

            return (List) JsonUtils.createCustomGson().fromJson(EntityUtils.toString(response.getEntity()), List.class);
        }
    }

    public List getClusterShardDetailSettings(UUID clusterId, String indexes) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", "/_cat/recovery/" + String.join(",", indexes));
            request.addParameter("format", "json");
            Response response = client.getLowLevelClient().performRequest(request);

            return (List) JsonUtils.createCustomGson().fromJson(EntityUtils.toString(response.getEntity()), List.class);
        }
    }

    public Map<String, Object> getClusterSettings(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", "/_cluster/settings");
            request.addParameter("flat_settings", "true");
            request.addParameter("filter_path", "transient");
            Response response = client.getLowLevelClient().performRequest(request);

            return (Map<String, Object>) JsonUtils.createCustomGson().fromJson(EntityUtils.toString(response.getEntity()), Map.class);
        }
    }

    public String removeNode(UUID clusterId, String body) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("PUT", "/_cluster/settings");
            request.setJsonEntity(body);
            Response response = client.getLowLevelClient().performRequest(request);

            return EntityUtils.toString(response.getEntity());
        }
    }
}
