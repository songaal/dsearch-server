package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.services.CollectionService;
import com.danawa.fastcatx.server.services.IndexingJobService;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

@SpringBootTest
public class CollectionTest {
    private static Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    @Autowired
    private IndexingJobService indexingJobService;
    @Autowired
    private ElasticsearchFactory elasticsearchFactory;
    @Autowired
    private CollectionService collectionService;
    @Test
    public void propagateTest() {
        UUID clusterId = UUID.fromString("a4ac402c-b18c-45b4-916a-88e09cc7165e");
//        String collectionId = "naOBcHMBfPWyeN6Dveab";
//        try {
//            Collection collection = collectionService.findById(clusterId, collectionId);
//            indexingJobService.propagate(clusterId, false, collection);
//        } catch (Exception e) {
//            logger.error("", e);
//        }



//        String
//
//        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//
//
//            SearchRequest searchRequest = new SearchRequest();
//            searchRequest.indices()
//
//            client.search()
//
//
//            indexingJobService.propagate(client, index);
//
//        } catch (Exception e) {
//            logger.error("", e);
//        }
    }

}
