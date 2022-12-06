package com.danawa.dsearch.server.elasticsearch;

import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.utils.JsonUtils;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

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
            Map<String, Object> fullSearchMap = JsonUtils.convertStringToMap(response.toString());
            Map<String, Object> searchHits = (Map<String, Object>) fullSearchMap.get("hits");
            return (List<Map<String, Object>>) searchHits.get("hits");
        }
    }

    public boolean isExistIndex(UUID clusterId, String index) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            GetIndexRequest getIndexRequest = new GetIndexRequest(index);
            return client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
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

                result.put(fieldName, termsList);
            }

            return result;
        }
    }

}
