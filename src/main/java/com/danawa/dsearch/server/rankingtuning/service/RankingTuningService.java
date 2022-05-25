package com.danawa.dsearch.server.rankingtuning.service;

import com.danawa.dsearch.server.config.ElasticsearchFactory;

import com.danawa.dsearch.server.rankingtuning.entity.RankingTuningRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;


@Service
public class RankingTuningService {
    private static Logger logger = LoggerFactory.getLogger(RankingTuningService.class);
    private final ElasticsearchFactory elasticsearchFactory;

    public RankingTuningService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public AnalyzeResponse getAnalyze(UUID clusterId, RankingTuningRequest rankingTuningRequest, String analyzer, String hitValue) throws IOException{
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(rankingTuningRequest.getIndex(), analyzer, hitValue);
            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
            return analyzeResponse;
        }
    }

    public SearchResponse getSearch(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(rankingTuningRequest.getIndex());

            String query = rankingTuningRequest.getText();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }

    public Map<String, Object> getMappings(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            String index = rankingTuningRequest.getIndex();
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> mappings = response.mappings();
            for(String key: mappings.keySet()){
                System.out.println(key);
            }
            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
            return result;
        }
    }

    public Map<String, MappingMetadata> getMultipleMappings(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            String index = rankingTuningRequest.getIndex();
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> mappings = response.mappings();
//            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
//            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;
            return mappings;
        }
    }

    public SearchResponse getMultipleSearch(UUID clusterId, RankingTuningRequest rankingTuningRequest) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(rankingTuningRequest.getIndex());
            String query = rankingTuningRequest.getText();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            return searchResponse;
        }
    }
    public AnalyzeResponse getMultipleAnalyze(UUID clusterId, String index, String analyzer, String hitValue) throws IOException{
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(index, analyzer, hitValue);
            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
            return analyzeResponse;
        }
    }
}
