//package com.danawa.dsearch.server.migration;
//
//import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
//import com.danawa.dsearch.server.migration.service.MigrationService;
//import com.danawa.dsearch.server.pipeline.service.PipelineService;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//import org.springframework.mock.web.MockMultipartFile;
//import org.springframework.web.multipart.MultipartFile;
//
//import java.nio.charset.StandardCharsets;
//import java.util.Map;
//import java.util.UUID;
//
//@ExtendWith(MockitoExtension.class)
//public class MigrationServiceTest {
//    @Mock
//    private ElasticsearchFactory elasticsearchFactory;
//    @Mock
//    private PipelineService pipelineService;
//
//    private MigrationService migrationService;
//
//    @BeforeEach
//    public void setup(){
//        migrationService = new FakeMigrationService(elasticsearchFactory, pipelineService);
//    }
//
//    @Test
//    @DisplayName("파일로 데이터 덮어쓰기 성공")
//    public void upload_file_success(){
//        UUID clusterId = UUID.randomUUID();
//        String name = "name";
//        MultipartFile file = new MockMultipartFile(name, "helloworld".getBytes(StandardCharsets.UTF_8));
//
//        Map<String, Object> result = migrationService.uploadFile(clusterId, file);
//
//        Assertions.assertEquals(true, result.get("result"));
//    }
//
//    @Test
//    @DisplayName("파일로 데이터 덮어쓰기 실패, 네트워크 상태나 ES의 상태가 좋지 않음")
//    public void upload_file_fail_when_something_network_is_wrong(){
//        UUID clusterId = UUID.randomUUID();
//        String name = "name";
//        MultipartFile file = new MockMultipartFile(name, (byte[]) null);
//
//        Map<String, Object> result = migrationService.uploadFile(clusterId, file);
//
//        Assertions.assertEquals(false, result.get("result"));
//    }
//}
