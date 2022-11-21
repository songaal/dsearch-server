package com.danawa.dsearch.server.tools;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.tools.entity.AnalysisToolRequest;
import com.danawa.dsearch.server.tools.service.ToolsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class FakeToolsService extends ToolsService {
    public FakeToolsService(String dictionaryIndex, ElasticsearchFactory elasticsearchFactory) {
        super(dictionaryIndex, elasticsearchFactory);
    }

    public String getPlugins(UUID clusterId) throws IOException {
        return "a\nb\nc";
    }
    public List<String> checkPlugins(UUID clusterId, Set<String> plugins) throws IOException{
        List<String> result = new ArrayList<>();
        result.add("plugin");
        return result;
    }
    public String getDetailAnalysis(UUID clusterId, AnalysisToolRequest request) throws IOException {
        return "쿼리 요청 후 결과 받아오기";
    }
}
