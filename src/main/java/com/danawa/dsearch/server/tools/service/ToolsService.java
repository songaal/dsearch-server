package com.danawa.dsearch.server.tools.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.tools.adapter.ToolsAdapter;
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
    private ToolsAdapter toolsAdapter;
    private String dictionaryIndex;

    public ToolsService(@Value("${dsearch.dictionary.index}") String dictionaryIndex,
                        ToolsAdapter toolsAdapter) {
        this.dictionaryIndex = dictionaryIndex;
        this.toolsAdapter = toolsAdapter;
    }

    public String getPlugins(UUID clusterId) throws IOException {
        return toolsAdapter.getPlugins(clusterId);
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
        try {
            String text = "Sandisk Extream Z80 USB 16gb";
            toolsAdapter.analysisTextUsingPlugin(clusterId, dictionaryIndex, pluginName, true, text);
            return true;
        } catch (ResponseException re) {
            logger.error("Plugin Check Fail: {}", re.getMessage());
            return false;
        }
    }

    public String getDetailAnalysis(UUID clusterId, AnalysisToolRequest request) throws IOException {
        String plugin = request.getPlugin();
        String text = request.getText();
        boolean useForQuery = Objects.isNull(request.getUseForQuery()) ? false : Boolean.parseBoolean(request.getUseForQuery());
        return toolsAdapter.analysisTextUsingPlugin(clusterId, dictionaryIndex, plugin, useForQuery, text);
    }
}
