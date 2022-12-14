package com.danawa.dsearch.server.document.service;

import com.danawa.dsearch.server.document.dto.DocumentAnalysisReqeust;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.utils.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doThrow;

@ExtendWith(MockitoExtension.class)
public class DocumentAnalysisServiceTests {
    private DocumentAnalysisService documentAnalysisService;

    @Mock
    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    @BeforeEach
    public void setup(){
        this.documentAnalysisService = new DocumentAnalysisService(elasticsearchFactoryHighLevelWrapper);
    }

    @Test
    @DisplayName("문서 분석 성공")
    public void analysis_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String index = "test";
        String name = "test";
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match_all\": {}\n" +
                "  }\n" +
                "}\n";

        // given
        Map<String, Object> mappings = JsonUtils.convertStringToMap("{\n" +
                "  \"test\" : {\n" +
                "      \"properties\" : {\n" +
                "        \"productName\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"whitespace_lowercase\"\n" +
                "        },\n" +
                "        \"productName2\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"whitespace_lowercase\"\n" +
                "        },\n" +
                "        \"productName3\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"whitespace_lowercase\"\n" +
                "        },\n" +
                "        \"productName4\" : {\n" +
                "          \"type\" : \"text\",\n" +
                "          \"analyzer\" : \"whitespace_lowercase\"\n" +
                "        },\n" +
                "        \"productName5\" : {\n" +
                "          \"type\" : \"text\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "}");
        List<Map<String, Object>> searchList = new ArrayList<>();
        searchList.add(JsonUtils.convertStringToMap("{\n" +
                "        \"_index\" : \"test\",\n" +
                "        \"_type\" : \"_doc\",\n" +
                "        \"_id\" : \"bXXT8IQBPUNdwAk1KNQv\",\n" +
                "        \"_score\" : 1.0,\n" +
                "        \"_source\" : {\n" +
                "          \"productName\" : \"아파치 USB 온열매트 전기방석\"\n" +
                "        }\n" +
                "      }"));

        Map<String, Object> termVectors = new HashMap<>();
        termVectors.put("productName", "usb, 아파치, 온열매트, 전기방석");

        DocumentAnalysisReqeust documentAnalysisReqeust = new DocumentAnalysisReqeust();
        documentAnalysisReqeust.setIndex(index);
        documentAnalysisReqeust.setName(name);
        documentAnalysisReqeust.setQuery(query);

        given(elasticsearchFactoryHighLevelWrapper.getIndexMappings(clusterId, index)).willReturn(mappings);
        given(elasticsearchFactoryHighLevelWrapper.search(eq(clusterId), eq(index), any(String.class))).willReturn(searchList);
        given(elasticsearchFactoryHighLevelWrapper.getTermVectors(eq(clusterId), eq(index), eq("bXXT8IQBPUNdwAk1KNQv"), any(String[].class))).willReturn(termVectors);

        Map<String, Object> analyzedResult = this.documentAnalysisService.analyzeDocument(clusterId, documentAnalysisReqeust);
        Map<String, Object> analyzedData = (Map<String, Object>)analyzedResult.get("analysis");
        Map<String, Object> analyzedFieldsData = (Map<String, Object>)analyzedData.get("bXXT8IQBPUNdwAk1KNQv");
        Map<String, Object> fieldData = (Map<String, Object>)analyzedFieldsData.get("productName");

        String document = (String) fieldData.get("document");
        String documentTermVectors =(String) fieldData.get("documentTermVectors");


        assertThat("document는 최소 빈 값이 아님", document.length(), greaterThan(0));
        assertThat("따라서 termvector도 최소 빈 값이 아님", documentTermVectors.length(), greaterThan(0));
    }

    @Test
    @DisplayName("문서 분석 실패 - 필드에 분석기 셋팅이 되지 않음")
    public void analysis_fail_case1() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String index = "test";
        String name = "test";
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match_all\": {}\n" +
                "  }\n" +
                "}\n";

        // given
        Map<String, Object> mappings = JsonUtils.convertStringToMap("{\n" +
                "  \"test\" : {\n" +
                "      \"properties\" : {\n" +
                "        \"productName\" : {\n" +
                "          \"type\" : \"text\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "}");
        List<Map<String, Object>> searchList = new ArrayList<>();
        searchList.add(JsonUtils.convertStringToMap("{\n" +
                "        \"_index\" : \"test\",\n" +
                "        \"_type\" : \"_doc\",\n" +
                "        \"_id\" : \"bXXT8IQBPUNdwAk1KNQv\",\n" +
                "        \"_score\" : 1.0,\n" +
                "        \"_source\" : {\n" +
                "          \"productName\" : \"아파치 USB 온열매트 전기방석\"\n" +
                "        }\n" +
                "      }"));


        DocumentAnalysisReqeust documentAnalysisReqeust = new DocumentAnalysisReqeust();
        documentAnalysisReqeust.setIndex(index);
        documentAnalysisReqeust.setName(name);
        documentAnalysisReqeust.setQuery(query);

        given(elasticsearchFactoryHighLevelWrapper.getIndexMappings(clusterId, index)).willReturn(mappings);

        Map<String, Object> analyzedResult = this.documentAnalysisService.analyzeDocument(clusterId, documentAnalysisReqeust);
        Map<String, Object> analyzedData = (Map<String, Object>)analyzedResult.get("analysis");

        Assertions.assertEquals(analyzedData.size(), 0);
    }

    @Test
    @DisplayName("문서 분석 실패 - ES 연결 실패")
    public void analysis_fail_case2() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String index = "test";
        String name = "test";
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"match_all\": {}\n" +
                "  }\n" +
                "}\n";

        DocumentAnalysisReqeust documentAnalysisReqeust = new DocumentAnalysisReqeust();
        documentAnalysisReqeust.setIndex(index);
        documentAnalysisReqeust.setName(name);
        documentAnalysisReqeust.setQuery(query);

        doThrow(IOException.class).when(elasticsearchFactoryHighLevelWrapper).getIndexMappings(eq(clusterId), eq(index));
        Map<String, Object> analyzedResult = this.documentAnalysisService.analyzeDocument(clusterId, documentAnalysisReqeust);
        Map<String, Object> analyzedData = (Map<String, Object>)analyzedResult.get("analysis");

        Assertions.assertEquals(analyzedData.size(), 0);
    }
}
