package com.danawa.dsearch.server.elasticsearch;

import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
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

}
