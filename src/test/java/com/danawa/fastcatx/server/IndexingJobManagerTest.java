package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.services.IndexingJobManager;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@SpringBootTest
public class IndexingJobManagerTest {
    private static Logger logger = LoggerFactory.getLogger(IndexingJobManagerTest.class);

    @Autowired
    private IndexingJobManager indexingJobManager;
    @Autowired
    private ElasticsearchFactory elasticsearchFactory;

//    >>> .fastcatx_index_history
//- docSize: 문서 수
//- endTime: 색인 종료시간
//- index: 인덱스명
//- autoRun: 자동 or 스케쥴 실행여부
//- startTime: 색인 시작시간
//- status: SUCCESS or FAIL
//- storage: 용량
//- jobType: 색인 (전체색인:FULL_INDEX or 동적색인:DYNAMIC_INDEX) or 전파:PROPAGATE or 교체:EXPOSE
//
//
//>>> .fastcatx_last_index_status
//- index: 인덱스명
//- startTime: 시작시간
//- status: 상태 ( RUNNING 뿐... )

    @Test
    public void addHistoryTest() {
        UUID clusterId = UUID.fromString("fd6ac513-27d5-455e-9af0-ed8f8ac0389e");
        String index = "example1";
        String jobType = "FULL_INDEX";
        long startTime = System.currentTimeMillis() - 3600 * 1000;
        long endTime = System.currentTimeMillis();
        boolean autoRun = true;
        String status = "SUCCESS";

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", String.format("/_cat/indices/%s", index));
            request.addParameter("format", "json");
            request.addParameter("h", "store.size,docs.count");
            Response response = client.getLowLevelClient().performRequest(request);
            String responseBodyString = EntityUtils.toString(response.getEntity());
            List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
            Map<String, Object> catIndex = catIndices.get(0);
            String store = (String) catIndex.get("store.size");
            String docSize = (String) catIndex.get("docs.count");
            indexingJobManager.addIndexHistory(client, index, jobType, startTime, endTime, autoRun, docSize, status, store);
        } catch(Exception e) {
            logger.error("addIndexHistory >> clusterId: {}, index: {}", clusterId, index, e);
        }
    }

    @Test
    public void addLastIndexStatusTest() {
        UUID clusterId = UUID.fromString("fd6ac513-27d5-455e-9af0-ed8f8ac0389e");
        String index = "example1";
        long startTime = 1595305624824L;
        String status = "RUNNING";

//        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            indexingJobManager.addLastIndexStatus(client, index, startTime, status);
//        } catch(Exception e) {
//            logger.error("addIndexHistory >> clusterId: {}, index: {}", clusterId, index, e);
//        }
    }

    @Test
    public void deleteLastIndexStatusTest() {
        UUID clusterId = UUID.fromString("fd6ac513-27d5-455e-9af0-ed8f8ac0389e");
        String index = "example1";
        long startTime = 1595305624824L;
        String status = "RUNNING";

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            indexingJobManager.deleteLastIndexStatus(client, index, startTime);
        } catch(Exception e) {
            logger.error("addIndexHistory >> clusterId: {}, index: {}", clusterId, index, e);
        }
    }

}
