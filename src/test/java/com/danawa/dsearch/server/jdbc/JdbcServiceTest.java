package com.danawa.dsearch.server.jdbc;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.action.search.SearchResponse;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class JdbcServiceTest {
    private JdbcService jdbcService;
    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    @Mock
    private IndicesService indicesService;
    private String jdbcIndex = ".dsearch_jdbc";

    @BeforeEach
    public void setup() {
        this.jdbcService = new FakeJdbcService(jdbcIndex, indicesService, elasticsearchFactory);
    }

    @Test
    @DisplayName("시스템 인덱스 생성 테스트 안함. (여기서 진행하지 않음)")
    public void fetchSystemIndex() {
    }

    @Test
    @DisplayName("JDBC 커넥션 테스트 성공")
    public void connection_test_success(){
        JdbcRequest request = new JdbcRequest();
        request.setAddress("localhost");

        boolean result = jdbcService.connectionTest(request);

        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("JDBC 커넥션 테스트 실패")
    public void connection_test_fail(){
        JdbcRequest request = new JdbcRequest();
        request.setAddress("other");

        boolean result = jdbcService.connectionTest(request);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName("JDBC 리스트 가져오기 성공")
    public void get_jdbc_list_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        List<Map<String, Object>> result  = jdbcService.getJdbcList(clusterId);
        Assertions.assertEquals(0, result.size());
    }

    @Test
    @DisplayName("JDBC 리스트 가져오기 실패")
    public void get_jdbc_list_fail()  {
        Assertions.assertThrows(IOException.class, () -> {
            List<Map<String, Object>> result  = jdbcService.getJdbcList(null);
        });
    }

    @Test
    @DisplayName("JDBC 추가 하기 성공")
    public void add_jdbc_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        JdbcRequest jdbcRequest = new JdbcRequest();

        boolean result = jdbcService.addJdbcSource(clusterId, jdbcRequest);

        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("JDBC 추가 하기 실패, cluster id 가 없을 때")
    public void add_jdbc_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            JdbcRequest jdbcRequest = new JdbcRequest();
            boolean result = jdbcService.addJdbcSource(null, jdbcRequest);
        });
    }

    @Test
    @DisplayName("JDBC 추가 하기 실패, jdbcRequest 가 없을 때")
    public void add_jdbc_fail_when_jdbcRequest_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            boolean result = jdbcService.addJdbcSource(clusterId, null);
        });
    }

    @Test
    @DisplayName("JDBC 삭제하기 성공")
    public void delete_jdbc_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String id = "id";

        boolean result = jdbcService.deleteJdbcSource(clusterId, id);

        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("JDBC 삭제 하기 실패, id가 빈값 일 때")
    public void delete_jdbc_fail_when_id_is_0_length() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String id = "";

        boolean result = jdbcService.deleteJdbcSource(clusterId, id);

        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName("JDBC 삭제 하기 실패, clusterId가 null 일 경우")
    public void delete_jdbc_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            String id = "id";
            boolean result = jdbcService.deleteJdbcSource(null, id);
        });
    }

    @Test
    @DisplayName("JDBC 삭제 하기 실패, id가 null 일 경우")
    public void delete_jdbc_fail_when_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            boolean result = jdbcService.deleteJdbcSource(clusterId, null);
        });
    }

    @Test
    @DisplayName("JDBC 수정하기 성공")
    public void update_jdbc_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String id = "id";
        JdbcRequest request = new JdbcRequest();

        boolean result = jdbcService.updateJdbcSource(clusterId, id, request);

        Assertions.assertTrue(result);
    }

    @Test
    @DisplayName("JDBC 수정하기 실패, clusterId 가 Null 일 경우")
    public void update_jdbc_fail_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            JdbcRequest request = new JdbcRequest();

            boolean result = jdbcService.updateJdbcSource(null, id, request);
        });
    }

    @Test
    @DisplayName("JDBC 수정하기 실패, id 가 Null 일 경우")
    public void update_jdbc_fail_id_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            JdbcRequest request = new JdbcRequest();

            boolean result = jdbcService.updateJdbcSource(clusterId, null, request);
        });
    }

    @Test
    @DisplayName("JDBC 수정하기 실패, JdbcRequest 가 Null 일 경우")
    public void update_jdbc_fail_jdbcRequest_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String id = "id";
            JdbcRequest request = new JdbcRequest();

            boolean result = jdbcService.updateJdbcSource(clusterId, id, null);
        });
    }

    @Test
    @DisplayName("JDBC 수정하기 실패, id 가 빈 값 일 경우")
    public void update_jdbc_fail_id_is_0_length() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String id = "";
        JdbcRequest request = new JdbcRequest();

        boolean result = jdbcService.updateJdbcSource(clusterId, id, request);
        Assertions.assertFalse(result);
    }

    @Test
    @DisplayName("JDBC 내용 다운로드 성공")
    public void download_success() throws IOException {
        UUID clusterId = UUID.fromString("4d503fa9-1a8c-444c-891e-4553a2f189e6");
        Map<String, Object> message = new HashMap<>();
        String result = jdbcService.download(clusterId, message);

        Assertions.assertTrue(Matchers.greaterThan(0).matches(result.length()));
        Assertions.assertNotNull(message.get("jdbc"));
    }

    @Test
    @DisplayName("JDBC 내용 다운로드 실패, clusterId 가 null 일 경우")
    public void download_fail_when_clusterId_is_null()  {
        Map<String, Object> message = new HashMap<>();
        String result = jdbcService.download(null, message);

        Assertions.assertEquals(0, result.length());
        Assertions.assertNull(message.get("jdbc"));
    }

    @Test
    @DisplayName("JDBC 내용 다운로드 실패, message 가 null 일 경우")
    public void download_fail_when_message_is_null()  {
        UUID clusterId = UUID.randomUUID();
        String result = jdbcService.download(clusterId, null);

        Assertions.assertEquals(0, result.length());
    }
}