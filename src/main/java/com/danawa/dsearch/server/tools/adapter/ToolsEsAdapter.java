package com.danawa.dsearch.server.tools.adapter;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class ToolsEsAdapter implements ToolsAdapter{
    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    ToolsEsAdapter(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    @Override
    public String getPlugins(UUID clusterId) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.getPlugins(clusterId);
    }

    @Override
    public String analysisTextUsingPlugin(UUID clusterId, String index, String pluginName, boolean useForQuery, String text) throws IOException {
        return elasticsearchFactoryHighLevelWrapper.analysisTextUsingPlugin(clusterId, index, pluginName, useForQuery, text);
    }
}
