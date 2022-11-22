package com.danawa.dsearch.server.collections.service.indexing;


import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.notice.AlertService;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class IndexingJobServiceTests {
    private IndexingJobService indexingJobService;

    private String jdbcSystemIndex = ".dsearch_jdbc";

    @Mock
    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;
    @Mock
    private HistoryService indexHistoryService;
    @Mock
    private AlertService alertService;
    @Mock
    private IndexerClient indexerClient;

    @BeforeEach
    public void setup(){
        this.indexingJobService = new IndexingJobService(
                elasticsearchFactoryHighLevelWrapper, jdbcSystemIndex,indexHistoryService, alertService, indexerClient);
    }

    @Test
    @DisplayName("색인 테스트 - 인덱스 2개가 다 있는 경우")
    public void indexingTest(){
        // 정상 작동
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setBaseId("collection");
        Collection.Index indexA = new Collection.Index();
        indexA.setIndex("indexA");
        Collection.Index indexB = new Collection.Index();
        indexB.setIndex("indexB");
        collection.setIndexA(indexA);
        collection.setIndexB(indexB);
        Collection.Launcher launcher = new Collection.Launcher();
        launcher.setYaml("scheme: http");
        collection.setLauncher(launcher);
        collection.setJdbcId("1");

        Queue<IndexActionStep> actionSteps = new ArrayDeque<>();
        actionSteps.add(IndexActionStep.FULL_INDEX);

        String indexingJobId = "indexingJobId";

        Map<String, Object> jdbcContent = new HashMap<>();

        Assertions.assertDoesNotThrow(() -> {
//            given(elasticsearchFactoryHighLevelWrapper.getAliases(clusterId)).willReturn(aliases);
//            given(elasticsearchFactoryHighLevelWrapper.createIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
//            given(elasticsearchFactoryHighLevelWrapper.updateIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
            given(elasticsearchFactoryHighLevelWrapper.getIndexDocument(eq(clusterId), any(String.class), any(String.class))).willReturn(jdbcContent);
            given(indexerClient.startJob(any(Map.class), any(Collection.class))).willReturn(indexingJobId);

            IndexingStatus indexingStatus = this.indexingJobService.indexing(clusterId, collection, actionSteps );
            Assertions.assertEquals(indexingJobId, indexingStatus.getIndexingJobId());
        });

        // indexA에 별칭이 있을 경우
        Map<String, Object> aliases = new HashMap<>();
        aliases.put("index", new HashMap<>());
        indexA.setAliases(aliases);
        Assertions.assertDoesNotThrow(() -> {
            String aliasString = "{\n" +
                    "  \"index\" : {\n" +
                    "    \"aliases\" : {\n" +
                    "      \"index-a\" : {\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }}";
            given(elasticsearchFactoryHighLevelWrapper.getIndexDocument(eq(clusterId), any(String.class), any(String.class))).willReturn(jdbcContent);
            given(indexerClient.startJob(any(Map.class), any(Collection.class))).willReturn(indexingJobId);

            IndexingStatus indexingStatus = this.indexingJobService.indexing(clusterId, collection, actionSteps );
            Assertions.assertEquals(indexingJobId, indexingStatus.getIndexingJobId());
        });
    }

    @Test
    @DisplayName("색인 테스트 - 인덱스A 한개만 있는 경우")
    public void indexingTest_exist_indexA(){
        // 정상 작동
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setBaseId("collection");
        Collection.Index indexA = new Collection.Index();
        indexA.setIndex("indexA");
        collection.setIndexA(indexA);
        Collection.Launcher launcher = new Collection.Launcher();
        launcher.setYaml("scheme: http");
        collection.setLauncher(launcher);
        collection.setJdbcId("1");

        Queue<IndexActionStep> actionSteps = new ArrayDeque<>();
        actionSteps.add(IndexActionStep.FULL_INDEX);

        String indexingJobId = "indexingJobId";
        String aliases = "";
        Map<String, Object> jdbcContent = new HashMap<>();

        Assertions.assertDoesNotThrow(() -> {
//            given(elasticsearchFactoryHighLevelWrapper.getAliases(clusterId)).willReturn(aliases);
//            given(elasticsearchFactoryHighLevelWrapper.createIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
//            given(elasticsearchFactoryHighLevelWrapper.updateIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
            given(elasticsearchFactoryHighLevelWrapper.getIndexDocument(eq(clusterId), any(String.class), any(String.class))).willReturn(jdbcContent);
            given(indexerClient.startJob(any(Map.class), any(Collection.class))).willReturn(indexingJobId);

            IndexingStatus indexingStatus = this.indexingJobService.indexing(clusterId, collection, actionSteps );
            Assertions.assertEquals(indexingJobId, indexingStatus.getIndexingJobId());
        });
    }

    @Test
    @DisplayName("색인 테스트 - 인덱스B만 있는 경우")
    public void indexingTest_exist_indexB(){
        // 정상 작동
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setBaseId("collection");
        Collection.Index indexB = new Collection.Index();
        indexB.setIndex("indexB");
        collection.setIndexB(indexB);
        Collection.Launcher launcher = new Collection.Launcher();
        launcher.setYaml("scheme: http");
        collection.setLauncher(launcher);
        collection.setJdbcId("1");

        Queue<IndexActionStep> actionSteps = new ArrayDeque<>();
        actionSteps.add(IndexActionStep.FULL_INDEX);

        String indexingJobId = "indexingJobId";
        String aliases = "";
        Map<String, Object> jdbcContent = new HashMap<>();

        Assertions.assertDoesNotThrow(() -> {
//            given(elasticsearchFactoryHighLevelWrapper.getAliases(clusterId)).willReturn(aliases);
//            given(elasticsearchFactoryHighLevelWrapper.createIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
//            given(elasticsearchFactoryHighLevelWrapper.updateIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
            given(elasticsearchFactoryHighLevelWrapper.getIndexDocument(eq(clusterId), any(String.class), any(String.class))).willReturn(jdbcContent);
            given(indexerClient.startJob(any(Map.class), any(Collection.class))).willReturn(indexingJobId);

            IndexingStatus indexingStatus = this.indexingJobService.indexing(clusterId, collection, actionSteps );
            Assertions.assertEquals(indexingJobId, indexingStatus.getIndexingJobId());
        });
    }

    @Test
    @DisplayName("새로 고침 간격 변경 - 기본 셋팅")
    public void change_refresh_interval_index_default(){
        // 정상 작동
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setBaseId("collection");
        Collection.Index indexB = new Collection.Index();
        indexB.setIndex("indexB");
        collection.setIndexB(indexB);

        String targetIndex = "target";

        Assertions.assertDoesNotThrow(() -> {
            given(elasticsearchFactoryHighLevelWrapper.updateIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
            this.indexingJobService.changeRefreshInterval(clusterId, collection, targetIndex);
            verify(elasticsearchFactoryHighLevelWrapper, times(1)).updateIndexSettings(eq(clusterId), eq(targetIndex), any(Map.class));
        });
    }

    @Test
    @DisplayName("교체 테스트")
    public void exposeTest(){
        // 정상 작동
        UUID clusterId = UUID.randomUUID();
        Collection collection = new Collection();
        collection.setBaseId("collection");
        Collection.Index indexA = new Collection.Index();
        indexA.setIndex("indexA");
        indexA.setUuid(UUID.randomUUID().toString());
        collection.setIndexA(indexA);

        String targetIndex = "indexA";

        Assertions.assertDoesNotThrow(() -> {
            doNothing().when(elasticsearchFactoryHighLevelWrapper).updateAliases(eq(clusterId), any(IndicesAliasesRequest.class));
            given(elasticsearchFactoryHighLevelWrapper.updateIndexSettings(eq(clusterId), any(String.class), any(Map.class))).willReturn(true);
            this.indexingJobService.expose(clusterId, collection, targetIndex);
            verify(elasticsearchFactoryHighLevelWrapper, times(1)).updateIndexSettings(eq(clusterId), eq(targetIndex), any(Map.class));
        });
    }
}
