package com.danawa.dsearch.server.pipeline;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.pipeline.service.PipelineService;
import org.apache.commons.lang.NullArgumentException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
public class PipelineServiceTest {

    private PipelineService pipelineService;
    @Mock
    private ElasticsearchFactory elasticsearchFactory;

    @BeforeEach
    public void setup() {
        this.pipelineService = new FakePipelineService(elasticsearchFactory);
    }

    @Test
    @DisplayName("파이프라인 리스트 가져오기 성공")
    public void get_pipeline_lists_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String pipelines = pipelineService.getPipeLineLists(clusterId);

        Assertions.assertTrue(Matchers.greaterThan(0).matches(pipelines.length()));
    }

    @Test
    @DisplayName("파이프라인 리스트 가져오기 실패, 클러스터 Id가 null로 셋팅되어 들어감")
    public void get_pipeline_lists_fail() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            String pipelines = pipelineService.getPipeLineLists(null);
        });
    }

    @Test
    @DisplayName("파이프라인 한개 가져오기 성공")
    public void get_pipeline_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String name = "name";
        String pipelines = pipelineService.getPipeLine(clusterId, name);

        Assertions.assertTrue(Matchers.greaterThan(0).matches(pipelines.length()));
    }

    @Test
    @DisplayName("파이프라인 한 개 가져올 때, clusterId가 Null 일 경우 실패")
    public void get_pipeline_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String pipelines = pipelineService.getPipeLine(null, name);
        });
    }

    @Test
    @DisplayName("파이프라인 하나를 가져올 때, 파이프라인 명칭이 Null 일 경우 실패 ")
    public void get_pipeline_fail_when_name_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String pipelines = pipelineService.getPipeLine(clusterId, null);
        });
    }

    @Test
    @DisplayName("파이프라인 내용을 가져올 때, 파이프라인 이름이 빈 문자열일 경우 실패")
    public void get_pipeline_fail_when_name_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String pipelines = pipelineService.getPipeLine(clusterId, "");
        });
    }

    @Test
    @DisplayName("파이프라인 추가 성공")
    public void add_pipeline_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String name = "name";
        String body = "body";
        String pipelines = pipelineService.addPipeLine(clusterId, name, body);

        Assertions.assertTrue(Matchers.greaterThan(0).matches(pipelines.length()));
    }

    @Test
    @DisplayName("파이프라인을 추가할 때, clusterId가 Null 일 경우 실패 ")
    public void add_pipeline_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";

            String pipelines = pipelineService.addPipeLine(null, name, body);
        });
    }

    @Test
    @DisplayName("파이프라인을 추가할 때, pipeline name이 Null 일 경우 실패 ")
    public void add_pipeline_fail_when_pipeline_name_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";

            String pipelines = pipelineService.addPipeLine(clusterId, null, body);
        });
    }

    @Test
    @DisplayName("파이프라인을 추가할 때, pipeline name이 빈 문자열 일 경우 실패 ")
    public void add_pipeline_fail_when_pipeline_name_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";

            String pipelines = pipelineService.addPipeLine(clusterId, "", body);
        });
    }

    @Test
    @DisplayName("파이프라인을 추가할 때, pipeline body가 null 일 경우 실패 ")
    public void add_pipeline_fail_when_pipeline_body_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";

            String pipelines = pipelineService.addPipeLine(clusterId, name, null);
        });
    }

    @Test
    @DisplayName("파이프라인을 추가할 때, pipeline body가 빈 문자열 일 경우 실패 ")
    public void add_pipeline_fail_when_pipeline_body_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";

            String pipelines = pipelineService.addPipeLine(clusterId, name, "");
        });
    }

    @Test
    @DisplayName("파이프라인 테스트 진행 성공")
    public void test_pipeline_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String name = "name";
        String body = "body";
        boolean detail = true;
        String pipelines = pipelineService.testPipeline(clusterId, name, body, detail);

        Assertions.assertTrue(Matchers.greaterThan(0).matches(pipelines.length()));
    }

    @Test
    @DisplayName("파이프라인을 테스트 할 때, clusterId가 null 일 경우 실패 ")
    public void test_pipeline_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";
            boolean detail = true;
            String pipelines = pipelineService.testPipeline(null, name, body, detail);
        });
    }

    @Test
    @DisplayName("파이프라인을 테스트 할 때, pipeline 이름이 null 일 경우 실패 ")
    public void test_pipeline_fail_when_pipeline_name_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";
            boolean detail = true;
            String pipelines = pipelineService.testPipeline(clusterId, null, body, detail);
        });
    }
    @Test
    @DisplayName("파이프라인을 테스트 할 때, pipeline 이름이 빈 문자열 일 경우 실패 ")
    public void test_pipeline_fail_when_pipeline_name_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";
            boolean detail = true;
            String pipelines = pipelineService.testPipeline(clusterId, "", body, detail);
        });
    }
    @Test
    @DisplayName("파이프라인을 테스트 할 때, pipeline body가 null 일 경우 실패 ")
    public void test_pipeline_fail_when_pipeline_body_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";
            boolean detail = true;
            String pipelines = pipelineService.testPipeline(clusterId, name, null, detail);
        });
    }
    @Test
    @DisplayName("파이프라인을 테스트 할 때, pipeline body가 빈 문자열 일 경우 실패 ")
    public void test_pipeline_fail_when_pipeline_body_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String body = "body";
            boolean detail = true;
            String pipelines = pipelineService.testPipeline(clusterId, name, "", detail);
        });
    }

    @Test
    @DisplayName("파이프라인 삭제 성공")
    public void delete_pipeline_success() throws IOException {
        UUID clusterId = UUID.randomUUID();
        String name = "name";
        String pipelines = pipelineService.deletePipeLine(clusterId, name);

        Assertions.assertTrue(Matchers.greaterThan(0).matches(pipelines.length()));
    }

    @Test
    @DisplayName("파이프라인을 삭제 할 때, clusterId가  Null 일 경우 실패 ")
    public void delete_pipeline_fail_when_clusterId_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String pipelines = pipelineService.getPipeLine(null, name);
        });
    }

    @Test
    @DisplayName("파이프라인 삭제 할 때, 파이프라인 name이 Null 일 경우 실패 ")
    public void delete_pipeline_fail_when_name_is_null() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String pipelines = pipelineService.getPipeLine(clusterId, null);
        });
    }
    @Test
    @DisplayName("파이프라인 삭제 할 때, 파이프라인 name이 빈 문자열 일 경우 실패 ")
    public void delete_pipeline_fail_when_name_is_0_length() throws IOException {
        Assertions.assertThrows(NullArgumentException.class, () -> {
            UUID clusterId = UUID.randomUUID();
            String name = "name";
            String pipelines = pipelineService.getPipeLine(clusterId, "");
        });
    }
}


