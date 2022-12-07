package com.danawa.dsearch.server.jdbc;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.service.JdbcServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JdbcServiceImplTest {
    private JdbcServiceImpl jdbcServiceImpl;
    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    @Mock
    private IndicesService indicesService;
    private String jdbcIndex = ".dsearch_jdbc";
    private String JDBC_JSON = "jdbc.json";
    @BeforeEach
    public void setup() {
//        this.jdbcServiceImpl = new JdbcServiceImpl(jdbcIndex, indicesService, elasticsearchFactory);
    }
//
//    @Test
//    @DisplayName("시스템 인덱스 생성 테스트 성공")
//    public void create_jdbc_system_index_success() throws IOException {
//        //given
//        UUID clusterId = UUID.randomUUID();
//        doNothing().when(indicesService).createSystemIndex(clusterId, jdbcIndex, JDBC_JSON);
//
//        //when
//        jdbcServiceImpl.initialize(clusterId);
//
//        //then
//        verify(indicesService, times(1)).createSystemIndex(clusterId, jdbcIndex, JDBC_JSON);
//    }
//
//    @Test
//    @DisplayName("JDBC 커넥션 테스트 성공")
//    public void connection_test_success(){
//        JdbcRequest request = new JdbcRequest();
//        request.setUrl("url");
//        request.setDriver("driver");
//        request.setUser("user");
//        request.setPassword("password");
//
//        boolean result = jdbcServiceImpl.isConnectable(request);
//
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 커넥션 테스트 실패, 필수 파라미터가 없을 경우")
//    public void connection_test_fail_when_required_params_not_found(){
//        JdbcRequest request = new JdbcRequest();
//        boolean result = jdbcServiceImpl.isConnectable(request);
//        Assertions.assertFalse(result);
//
//        request.setUrl("url");
//        result = jdbcServiceImpl.isConnectable(request);
//        Assertions.assertFalse(result);
//
//        request.setDriver("driver");
//        result = jdbcServiceImpl.isConnectable(request);
//        Assertions.assertFalse(result);
//
//        request.setUser("user");
//        result = jdbcServiceImpl.isConnectable(request);
//        Assertions.assertFalse(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 리스트 가져오기 성공")
//    public void get_jdbc_list_success() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//
//        RestHighLevelClient client = mock(RestHighLevelClient.class);
//        SearchResponse response = mock(SearchResponse.class);
//        SearchHits searchHits = mock(SearchHits.class);
//        SearchHit[] searchHit = new SearchHit[0];
//        given(elasticsearchFactory.getClient(clusterId)).willReturn(client);
//        given(client.search(any(SearchRequest.class), eq(RequestOptions.DEFAULT))).willReturn(response);
//        given(response.getHits()).willReturn(searchHits);
//        given(response.getHits().getHits()).willReturn(searchHit);
//
//        List<Map<String, Object>> result  = jdbcServiceImpl.findAll(clusterId);
//        Assertions.assertEquals(0, result.size());
//    }
//
//    @Test
//    @DisplayName("JDBC 리스트 가져오기 실패")
//    public void get_jdbc_list_fail()  {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            List<Map<String, Object>> result  = jdbcServiceImpl.findAll(null);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 추가 하기 성공")
//    public void add_jdbc_success() throws IOException {
//        //given
//        UUID clusterId = UUID.randomUUID();
//        JdbcRequest jdbcRequest = new JdbcRequest();
//
//        //when
//        boolean result = jdbcServiceImpl.create(clusterId, jdbcRequest);
//
//        //then
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 추가 하기 실패, cluster id 가 없을 때")
//    public void add_jdbc_fail_when_clusterId_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            JdbcRequest jdbcRequest = new JdbcRequest();
//            boolean result = jdbcServiceImpl.create(null, jdbcRequest);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 추가 하기 실패, jdbcRequest 가 없을 때")
//    public void add_jdbc_fail_when_jdbcRequest_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            boolean result = jdbcServiceImpl.create(clusterId, null);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 삭제하기 성공")
//    public void delete_jdbc_success() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        String id = "id";
//
//        boolean result = jdbcServiceImpl.delete(clusterId, id);
//
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 삭제 하기 실패, id가 빈값 일 때")
//    public void delete_jdbc_fail_when_id_is_0_length() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        String id = "";
//
//        boolean result = jdbcServiceImpl.delete(clusterId, id);
//
//        Assertions.assertFalse(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 삭제 하기 실패, clusterId가 null 일 경우")
//    public void delete_jdbc_fail_when_clusterId_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            String id = "id";
//            boolean result = jdbcServiceImpl.delete(null, id);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 삭제 하기 실패, id가 null 일 경우")
//    public void delete_jdbc_fail_when_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            boolean result = jdbcServiceImpl.delete(clusterId, null);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 수정하기 성공")
//    public void update_jdbc_success() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        String id = "id";
//        JdbcRequest request = new JdbcRequest();
//
//        boolean result = jdbcServiceImpl.update(clusterId, id, request);
//
//        Assertions.assertTrue(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 수정하기 실패, clusterId 가 Null 일 경우")
//    public void update_jdbc_fail_clusterId_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            JdbcRequest request = new JdbcRequest();
//
//            boolean result = jdbcServiceImpl.update(null, id, request);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 수정하기 실패, id 가 Null 일 경우")
//    public void update_jdbc_fail_id_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            JdbcRequest request = new JdbcRequest();
//
//            boolean result = jdbcServiceImpl.update(clusterId, null, request);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 수정하기 실패, JdbcRequest 가 Null 일 경우")
//    public void update_jdbc_fail_jdbcRequest_is_null() throws IOException {
//        Assertions.assertThrows(NullArgumentException.class, () -> {
//            UUID clusterId = UUID.randomUUID();
//            String id = "id";
//            JdbcRequest request = new JdbcRequest();
//
//            boolean result = jdbcServiceImpl.update(clusterId, id, null);
//        });
//    }
//
//    @Test
//    @DisplayName("JDBC 수정하기 실패, id 가 빈 값 일 경우")
//    public void update_jdbc_fail_id_is_0_length() throws IOException {
//        UUID clusterId = UUID.randomUUID();
//        String id = "";
//        JdbcRequest request = new JdbcRequest();
//
//        boolean result = jdbcServiceImpl.update(clusterId, id, request);
//        Assertions.assertFalse(result);
//    }
//
//    @Test
//    @DisplayName("JDBC 내용 다운로드 성공")
//    public void download_success() throws IOException {
//        UUID clusterId = UUID.fromString("4d503fa9-1a8c-444c-891e-4553a2f189e6");
//        Map<String, Object> message = new HashMap<>();
//        String result = jdbcServiceImpl.download(clusterId, message);
//
//        Assertions.assertTrue(Matchers.greaterThan(0).matches(result.length()));
//        Assertions.assertNotNull(message.get("jdbc"));
//    }
//
//    @Test
//    @DisplayName("JDBC 내용 다운로드 실패, clusterId 가 null 일 경우")
//    public void download_fail_when_clusterId_is_null()  {
//        Map<String, Object> message = new HashMap<>();
//        String result = jdbcServiceImpl.download(null, message);
//
//        Assertions.assertEquals(0, result.length());
//        Assertions.assertNull(message.get("jdbc"));
//    }
//
//    @Test
//    @DisplayName("JDBC 내용 다운로드 실패, message 가 null 일 경우")
//    public void download_fail_when_message_is_null()  {
//        UUID clusterId = UUID.randomUUID();
//        String result = jdbcServiceImpl.download(clusterId, null);
//
//        Assertions.assertEquals(0, result.length());
//    }
}