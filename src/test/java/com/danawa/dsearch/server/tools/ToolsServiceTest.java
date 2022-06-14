package com.danawa.dsearch.server.tools;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.tools.entity.AnalysisToolRequest;
import com.danawa.dsearch.server.tools.service.ToolsService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class ToolsServiceTest{
    private String dictionaryIndex = "dsearch_dict";
    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    private ToolsService toolsService;

    @BeforeEach
    public void setup(){
        this.toolsService = new FakeToolsService(dictionaryIndex, elasticsearchFactory);
    }

    @Test
    @DisplayName("플러그인 리스트 가져오기 성공")
    public void get_plugins_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String result = toolsService.getPlugins(clusterId);
        Assertions.assertTrue(Matchers.greaterThan(0).matches(result.length()));
    }

    @Test
    @DisplayName("플러그인이 정상적으로 작동하는지 체크 성공")
    public void check_plugin_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        Set<String> plugins = new HashSet<>();
        List<String> result = toolsService.checkPlugins(clusterId, plugins);
        Assertions.assertTrue(Matchers.greaterThan(0).matches(result.size()));
    }

    @Test
    @DisplayName("플러그인으로 쿼리 요청 보내기 성공")
    public void get_detail_analysis_to_plugin_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        AnalysisToolRequest request = new AnalysisToolRequest();
        String result = toolsService.getDetailAnalysis(clusterId, request);
        Assertions.assertTrue(Matchers.greaterThan(0).matches(result.length()));
    }
}
