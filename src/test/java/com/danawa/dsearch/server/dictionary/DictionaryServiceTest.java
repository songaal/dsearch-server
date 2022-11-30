package com.danawa.dsearch.server.dictionary;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.dictionary.entity.DictionarySetting;
import com.danawa.dsearch.server.dictionary.service.DictionaryService;
import com.danawa.dsearch.server.excpetions.ServiceException;
import com.danawa.dsearch.server.indices.entity.DocumentPagination;
import com.danawa.dsearch.server.indices.service.IndicesService;
import org.apache.http.StatusLine;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DictionaryServiceTest {
    private String dictionaryIndex = ".dsearch_dict";
    private String dictionaryApplyIndex = ".dsearch_dict_apply";
    private final String INDEX_JSON = "dictionary.json";
    private final String DICT_APPLY_JSON = "dictionary_apply.json";
    @Mock
    IndicesService indicesService ;
    @Mock
    ClusterService clusterService;
    @Mock
    ElasticsearchFactory elasticsearchFactory;

    private DictionaryService dictionaryService;

    @BeforeEach
    public void setup(){
        this.dictionaryService = new FakeDictionaryService(dictionaryIndex, dictionaryApplyIndex, indicesService, clusterService, elasticsearchFactory);
    }

    @Test
    @DisplayName("엘라스틱서치에서 인덱스 생성 성공")
    public void create_default_dsearch_system_index_success() throws IOException {
        //given
        UUID clusterId = UUID.randomUUID();
        doNothing().when(indicesService).createSystemIndex(clusterId, dictionaryIndex, INDEX_JSON);
        doNothing().when(indicesService).createSystemIndex(clusterId, dictionaryApplyIndex, DICT_APPLY_JSON);

        //when
        dictionaryService.initialize(clusterId);

        //then
        verify(indicesService).createSystemIndex(clusterId, dictionaryIndex, INDEX_JSON);
        verify(indicesService).createSystemIndex(clusterId, dictionaryApplyIndex, DICT_APPLY_JSON);
    }

    @Test
    @DisplayName("분석 플러그인 셋팅 가져오기 성공")
    public void get_analysis_plugin_settings_success() throws IOException {
        UUID clusterId = UUID.randomUUID();

        List<DictionarySetting> result = dictionaryService.getAnalysisPluginSettings(clusterId);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    @DisplayName("사전의 타입에 따라 인덱스 결과 페이지네이션 성공")
    public void dictionary_index_pagenation_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String type = "";
        long pageNum = 0L;
        long rowSize = 0L;
        boolean isMatch = true;
        String searchColumns = "";
        String value = "";

        Assertions.assertInstanceOf(DocumentPagination.class, dictionaryService.documentPagination(clusterId, type, pageNum, rowSize, isMatch, searchColumns, value));
    }

    @Test
    @DisplayName("사전 레코드 생성 성공")
    public void createDocument() throws IOException, ServiceException {
        //given
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> document = new HashMap<>();
        document.put("id", "id");
        document.put("type", "type");
        document.put("keyword", "keyword");
        document.put("value", "value");

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);

        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        TotalHits totalHits = mock(TotalHits.class);

        when(searchResponse.getHits()).thenReturn(searchHits);
        when(searchHits.getTotalHits()).thenReturn(totalHits);
        when(client.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(searchResponse);

        IndexResponse indexResponse = mock(IndexResponse.class);
        when(indexResponse.getResult()).thenReturn(DocWriteResponse.Result.CREATED);
        when(client.index(any(), eq(RequestOptions.DEFAULT))).thenReturn(indexResponse);

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        RestStatus status = mock(RestStatus.class);
        when(updateResponse.status()).thenReturn(status);
        when(status.getStatus()).thenReturn(200);
        when(client.update(any(), eq(RequestOptions.DEFAULT))).thenReturn(updateResponse);

        // when
        IndexResponse result = dictionaryService.createDocument(clusterId, document);

        // then
        Assertions.assertEquals(DocWriteResponse.Result.CREATED, result.getResult());
    }

    @Test
    @DisplayName("사전 레코드 삭제 성공")
    public void deleteDocument() throws IOException, ServiceException {
        //given
        UUID clusterId = UUID.randomUUID();
        String dictionary = "dict";
        String id = "id";

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);

        GetResponse getResponse = mock(GetResponse.class);
        Map<String, Object> sourceAsMap = new HashMap<>();
        sourceAsMap.put("type", dictionary);
        when(getResponse.getSourceAsMap()).thenReturn(sourceAsMap);
        when(client.get(any(), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);

        DeleteResponse deleteResponse = mock(DeleteResponse.class);
        when(deleteResponse.getResult()).thenReturn(DocWriteResponse.Result.DELETED);
        when(client.delete(any(), eq(RequestOptions.DEFAULT))).thenReturn(deleteResponse);

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        RestStatus status = mock(RestStatus.class);
        when(updateResponse.status()).thenReturn(status);
        when(status.getStatus()).thenReturn(200);
        when(client.update(any(), eq(RequestOptions.DEFAULT))).thenReturn(updateResponse);

        // when
        DeleteResponse result = dictionaryService.deleteDocument(clusterId, dictionary, id);

        // then
        Assertions.assertEquals(DocWriteResponse.Result.DELETED, result.getResult());
    }

    @Test
    @DisplayName("사전 레코드 업데이트 성공")
    public void update_dictionary_success() throws IOException {
        //given
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> request = new HashMap<>();
        request.put("type", "type");
        String id = "id";

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);

        GetResponse getResponse = mock(GetResponse.class);
        Map<String, Object> sourceAsMap = new HashMap<>();
        sourceAsMap.put("type", "type");
        when(getResponse.getSourceAsMap()).thenReturn(sourceAsMap);
        when(client.get(any(), eq(RequestOptions.DEFAULT))).thenReturn(getResponse);

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        RestStatus status = mock(RestStatus.class);
        when(updateResponse.status()).thenReturn(status);
        when(status.getStatus()).thenReturn(200);
        when(updateResponse.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);
        when(client.update(any(), eq(RequestOptions.DEFAULT))).thenReturn(updateResponse);

        // when
        UpdateResponse result = dictionaryService.updateDocument(clusterId, id, request);

        // then
        Assertions.assertEquals(DocWriteResponse.Result.UPDATED, result.getResult());
    }

    @Test
    @DisplayName("사전 찾기 성공")
    public void find_dictionary_success() throws IOException {
        //given
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> request = new HashMap<>();
        request.put("word", "word");
        String id = "id";

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);

        RestClient restClient = mock(RestClient.class);
        Response response = mock(Response.class);

        when(client.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenReturn(response);

        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(response.getStatusLine()).thenReturn(statusLine);

        // when
        Response result = dictionaryService.findDict(clusterId, request);

        // then
        Assertions.assertEquals(200, result.getStatusLine().getStatusCode());
    }

    @Test
    @DisplayName("원격 사전 서버 정보 가져오기 성공")
    public void get_remote_cluster_info_success() {
        //given
        UUID clusterId = UUID.randomUUID();
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);

        String host = "host";
        int port = 9200;
        String username = "username";
        String password = "password";

        Cluster remoteCluster = new Cluster();
        remoteCluster.setId(clusterId);
        remoteCluster.setHost(host);
        remoteCluster.setPort(port);
        remoteCluster.setUsername(username);
        remoteCluster.setPassword(password);
        when(clusterService.findById(clusterId)).thenReturn(remoteCluster);

        // when
        Map<String, Object> result = dictionaryService.getRemoteInfo(clusterId);

        // then
        Assertions.assertEquals(false, result.get("remote"));
        Assertions.assertEquals(host, result.get("host"));
        Assertions.assertEquals(port, result.get("port"));
        Assertions.assertEquals(username, result.get("username"));
        Assertions.assertEquals(password, result.get("password"));
        Assertions.assertEquals(clusterId, result.get("remoteClusterId"));
    }

    @Test
    @DisplayName("사전 컴파일 성공")
    public void compile_dictionary_success() throws IOException {
        //given
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> request = new HashMap<>();
        request.put("type", "type");

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);

        RestClient restClient = mock(RestClient.class);
        Response response = mock(Response.class);

        when(client.getLowLevelClient()).thenReturn(restClient);
        when(restClient.performRequest(any())).thenReturn(response);

        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(response.getStatusLine()).thenReturn(statusLine);

        Cluster remoteCluster = new Cluster();
        remoteCluster.setId(clusterId);
        when(clusterService.findById(clusterId)).thenReturn(remoteCluster);

        // when
        Response result = dictionaryService.compileDict(clusterId, request);

        // then
        Assertions.assertEquals(200, result.getStatusLine().getStatusCode());
    }

    @Test
    @DisplayName("사전 파일을 인덱스로 추가 성공")
    public void insert_dictionary_file_to_index_success() throws IOException {
        //given
        UUID clusterId = UUID.randomUUID();
        Map<String, Object> request = new HashMap<>();
        request.put("type", "type");

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);

        when(client.bulk(any(BulkRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(null);

        MultipartFile file = new MockMultipartFile("name", "helloworld".getBytes(StandardCharsets.UTF_8));
        String dictName = "";
        String dictType = "";
        List<String> fields = new ArrayList<>();

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        RestStatus status = mock(RestStatus.class);
        when(updateResponse.status()).thenReturn(status);
        when(status.getStatus()).thenReturn(200);
        when(client.update(any(), eq(RequestOptions.DEFAULT))).thenReturn(updateResponse);

        // when
        Map<String, Object> result = dictionaryService.insertDictFileToIndex(clusterId, dictName, dictType, file, fields);


        // then
        Assertions.assertEquals(true, result.get("result"));
    }

    @Test
    @DisplayName("사전 데이터 리셋 성공")
    public void reset_dictionary_success() throws IOException {
        //given
        UUID clusterId = UUID.randomUUID();

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);
        given(elasticsearchFactory.getDictionaryRemoteClusterId(clusterId)).willReturn(clusterId);

        when(client.deleteByQuery(any(DeleteByQueryRequest.class), eq(RequestOptions.DEFAULT))).thenReturn(null);

        UpdateResponse updateResponse = mock(UpdateResponse.class);
        RestStatus status = mock(RestStatus.class);
        when(updateResponse.status()).thenReturn(status);
        when(status.getStatus()).thenReturn(200);
        when(client.update(any(), eq(RequestOptions.DEFAULT))).thenReturn(updateResponse);

        // when
        boolean result = dictionaryService.resetDict(clusterId, "dictionaryName");

        //then
        Assertions.assertTrue(result);
    }
}
