package com.danawa.dsearch.server.elasticsearch;

import com.danawa.dsearch.server.utils.JsonUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;

import java.nio.charset.Charset;
import java.util.*;



/**
 * 엘라스틱서치 RestHighLevelClient에서 제공하지 않는 기능들을 모아서 제공.
 */
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

    public void createIndex(UUID clusterId, String index, String configurations) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            if (!client.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)) {
                client.indices().create(new CreateIndexRequest(index)
                                .source(StreamUtils.copyToString(new ClassPathResource(configurations).getInputStream(),
                                        Charset.defaultCharset()), XContentType.JSON),
                        RequestOptions.DEFAULT);
            }
        }
    }

    public String deleteIndex(UUID clusterId, String index, String id) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            // 1. request 만들기
            DeleteRequest deleteRequest = new DeleteRequest(index);
            deleteRequest.id(id);

            // 2. 삭제
            DeleteResponse response = client.delete(deleteRequest, RequestOptions.DEFAULT);
            return response.getResult().getLowercase();
        }
    }

    public IndexResponse insertDocument(UUID clusterId, String index, Map<String, Object> source) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.index(index).source(source, XContentType.JSON);

            return  client.index(indexRequest, RequestOptions.DEFAULT);
        }
    }

    public UpdateResponse updateDocument(UUID clusterId, String index, Map<String, Object> source) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.index(index);
            updateRequest.doc(source, XContentType.JSON);

            return client.update(updateRequest, RequestOptions.DEFAULT);
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

    public Map<String, Object> getIndexMappings(UUID clusterId, String index) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> result = new HashMap<>();

            GetMappingsRequest getMappingsRequest = new GetMappingsRequest();
            getMappingsRequest.indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> fieldMappings = response.mappings();

            for (String field: fieldMappings.keySet()){
                MappingMetadata mappingMetadata = fieldMappings.get(field);
                result.put(field, mappingMetadata.getSourceAsMap());
            }

            return result;
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

    public List<Map<String, Object>> search(UUID clusterId, String index, String body) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            Request request = new Request("POST", index + "/_search");
            request.setJsonEntity(body);

            Response response = client.getLowLevelClient().performRequest(request);
            Map<String, Object> fullSearchMap = JsonUtils.convertStringToMap(EntityUtils.toString(response.getEntity()));
            Map<String, Object> searchHits = (Map<String, Object>) fullSearchMap.get("hits");
            return (List<Map<String, Object>>) searchHits.get("hits");
        }
    }

    public Map<String, Object> searchForOneDocument(UUID clusterId, String index, String docId) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            Request request = new Request("GET", index + "/_doc/" + docId);
            Response response = client.getLowLevelClient().performRequest(request);
            Map<String, Object> fullSearchMap = JsonUtils.convertStringToMap(EntityUtils.toString(response.getEntity()));
            return (Map<String, Object>) fullSearchMap.get("_source");
        }
    }

    public boolean isExistIndex(UUID clusterId, String index) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            GetIndexRequest getIndexRequest = new GetIndexRequest(index);
            return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        }
    }

    public List<Map<String, Object>> getClusterShardSettings(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", "/_cat/shards");
            request.addParameter("h", "index,state");
            request.addParameter("format", "json");
            Response response = client.getLowLevelClient().performRequest(request);

            return (List<Map<String, Object>>) JsonUtils.createCustomGson().fromJson(EntityUtils.toString(response.getEntity()), List.class);
        }
    }

    public List<Map<String, Object>> getClusterShardDetailSettings(UUID clusterId, String indexes) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", "/_cat/recovery/" + String.join(",", indexes));
            request.addParameter("format", "json");
            Response response = client.getLowLevelClient().performRequest(request);

            return (List<Map<String, Object>>) JsonUtils.createCustomGson().fromJson(EntityUtils.toString(response.getEntity()), List.class);
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
    
    public Map<String, Object> getTermVectors(UUID clusterId, String index, String docId, String[] fields) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            Map<String, Object> result = new HashMap<>();

            TermVectorsRequest termVectorsRequest = new TermVectorsRequest(index, docId);
            termVectorsRequest.setFields(fields);
            TermVectorsResponse response = client.termvectors(termVectorsRequest, RequestOptions.DEFAULT);

            List<TermVectorsResponse.TermVector> termVectorList = response.getTermVectorsList();

            for(TermVectorsResponse.TermVector termVector : termVectorList){
                String fieldName = termVector.getFieldName();
                List<String> termsList = new ArrayList<>();

                for(TermVectorsResponse.TermVector.Term term: termVector.getTerms()){
                    termsList.add(term.getTerm());
                }

                result.put(fieldName, String.join(", ", termsList));
            }

            return result;
        }
    }
}
