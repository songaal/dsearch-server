package com.danawa.dsearch.server;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
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
public class ESSuccessTest {
    private static Logger logger = LoggerFactory.getLogger(ESSuccessTest.class);

    @Autowired
    private ElasticsearchFactory elasticsearchFactory;

    @Test
    public void stageCheckTest() {
        UUID clusterId = UUID.fromString("283adcc8-c7dc-4b19-a150-fcebff66751c");
        String index = "test-a";
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", String.format("/%s/_recovery", index));
            request.addParameter("format", "json");
            request.addParameter("filter_path", "**.shards.stage");
            Response response = client.getLowLevelClient().performRequest(request);
            String entityString = EntityUtils.toString(response.getEntity());
            Map<String ,Object> entityMap = new Gson().fromJson(entityString, Map.class);
            List<Map<String, Object>> shards = (List<Map<String, Object>>) ((Map) entityMap.get(index)).get("shards");
            boolean done = true;
            for (int i = 0; i < shards.size(); i++) {
                Map<String, Object> shard = shards.get(i);
                String stage = String.valueOf(shard.get("stage"));
                if (!"DONE".equalsIgnoreCase(stage)) {
                    done = false;
                    break;
                }
            }
            logger.debug("{}, {}", done, shards);
        }catch (Exception e) {
            logger.error("", e);
        }

    }


}
