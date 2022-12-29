package com.danawa.dsearch.server.indices.adapter;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class IndicesEsAdapter implements IndicesAdapter{
    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    IndicesEsAdapter(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    @Override
    public void createIndex(UUID clusterId, String index, String source) throws IOException {
        elasticsearchFactoryHighLevelWrapper.createIndex(clusterId, index, source);
    }

    @Override
    public void deleteIndex(UUID clusterId, String index) throws IOException {
        elasticsearchFactoryHighLevelWrapper.deleteIndex(clusterId, index);
    }

    @Override
    public SearchResponse getDocuments(UUID clusterId, SearchRequest searchRequest) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.search(clusterId, searchRequest);
    }

    @Override
    public Map<String, Object> getFieldsMappings(UUID clusterId, String index) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.getIndexMappings(clusterId, index);
    }

    @Override
    public List<AnalyzeResponse.AnalyzeToken> analyze(UUID clusterId, String index, String analyzer, String text) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.analyze(clusterId, index, analyzer, text);
    }

    @Override
    public SearchResponse findAll(UUID clusterId, String index) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.searchScroll(clusterId, index);
    }

}
