package com.danawa.dsearch.server.collections;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.collections.service.indexing.IndexingJobManager;
import com.danawa.dsearch.server.collections.service.indexing.IndexingJobService;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.CronParseException;
import com.danawa.dsearch.server.indices.service.IndicesService;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHits;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class CollectionServiceTest {
    private CollectionService collectionService;

    @Mock
    private ClusterService clusterService;

    private String collectionIndex = ".dsearch_collection";
    private String indexSuffixA = "-a";
    private String indexSuffixB = "-b";
    private final String COLLECTION_INDEX_JSON = "collection.json";
    @Mock
    private ElasticsearchFactory elasticsearchFactory;

    @Mock
    private IndicesService indicesService;

    @Mock
    private IndexingJobService indexingJobService;

    @BeforeEach
    public void setup(){
        this.collectionService = new CollectionService(clusterService,
                collectionIndex,
                indexSuffixA,
                indexSuffixB,
                elasticsearchFactory,
                indicesService,
                indexingJobService);
    }

    @Test
    @DisplayName("초기화 테스트")
    public void initializeTest() throws IOException {
        // given
        UUID clusterId = UUID.randomUUID();

        // when
        // 복잡하므로 Fake형태에서 가져와서 흉내만 냄
        collectionService.initialize(clusterId);

        // then
        verify(indicesService, times(1)).createSystemIndex(clusterId, collectionIndex, COLLECTION_INDEX_JSON);
    }

//    @Test
//    @DisplayName("생성 테스트")
//    public void createTest() throws IOException {
//        // given
//        UUID clusterId = UUID.randomUUID();
//        Collection collection = new Collection();
//        RestHighLevelClient restHighLevelClient = mock(RestHighLevelClient.class);
//        SearchResponse searchResponse = mock(SearchResponse.class);
//
//        when(elasticsearchFactory.getClient(clusterId)).thenReturn(restHighLevelClient);
//        when(restHighLevelClient.search(any(SearchRequest.class), RequestOptions.DEFAULT)).thenReturn(searchResponse);
//        when(searchResponse.getHits()).thenReturn(SearchHits.empty());
//
//
//
//        collectionService.create(clusterId, collection);
//
//        // then
//        verify(indicesService, times(1)).createSystemIndex(clusterId, collectionIndex, COLLECTION_INDEX_JSON);
//    }

}
