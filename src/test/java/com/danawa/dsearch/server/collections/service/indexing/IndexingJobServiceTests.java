package com.danawa.dsearch.server.collections.service.indexing;


import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.notice.AlertService;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Queue;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
public class IndexingJobServiceTests {
    private IndexingJobService indexingJobService;

    private String jdbcSystemIndex = ".dsearch_jdbc";

    @Mock
    private ElasticsearchFactory elasticsearchFactory;
    @Mock
    private HistoryService indexHistoryService;
    @Mock
    private AlertService alertService;
    @Mock
    private IndexerClient indexerClient;

    @BeforeEach
    public void setup(){
        this.indexingJobService = new IndexingJobService(
                elasticsearchFactory, jdbcSystemIndex,indexHistoryService, alertService, indexerClient);
    }

    @Test
    @DisplayName("색인 테스트")
    public void indexingTest(){
        // 정상 작동
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setBaseId("collection");
        Collection.Index indexA = new Collection.Index();
        Collection.Index indexB = new Collection.Index();
        collection.setIndexA(indexA);
        collection.setIndexB(indexB);
        Queue<IndexActionStep> actionSteps = null;

        RestHighLevelClient client = mock(RestHighLevelClient.class);
        RestClient lowLevelClient = mock(RestClient.class);
        Response response = mock(Response.class);
        String entity = "";

        Assertions.assertDoesNotThrow(() -> {
            given(this.elasticsearchFactory.getClient(clusterId)).willReturn(client);
            given(lowLevelClient.performRequest(any(Request.class))).willReturn(response);

            this.indexingJobService.indexing(clusterId, collection, actionSteps );
        });



    }
}
