package com.danawa.dsearch.server.collections.service.monitor;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class IndexStatusService {
    /**
     * 색인 상태에 대해서 관리하는 클래스 입니다.
     */

    private static final Logger logger = LoggerFactory.getLogger(IndexStatusService.class);

    private final ElasticsearchFactory elasticsearchFactory;

    private final String lastIndexStatusIndex = ".dsearch_last_index_status";

    public IndexStatusService(ElasticsearchFactory elasticsearchFactory){
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void createIndexStatus(IndexingStatus status, String currentStatus) throws IOException {
        UUID clusterId = status.getClusterId();
        String collectionId = status.getCollection().getId();
        long startTime = status.getStartTime();
        String index = status.getIndex();
        String step = status.getCurrentStep().name();
        String jobId = status.getIndexingJobId();

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            createIndexStatus(client, collectionId, index, startTime, currentStatus, step, jobId);
        }
    }

    private void createIndexStatus(RestHighLevelClient client, String collectionId, String index, long startTime, String status, String step, String jobId) throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put("collectionId", collectionId);
        source.put("index", index);
        source.put("startTime", startTime);
        source.put("status", status);
        source.put("step", step);
        source.put("jobId", jobId);
        client.index(new IndexRequest().index(lastIndexStatusIndex).source(source), RequestOptions.DEFAULT);
    }

    public void deleteIndexStatus(UUID clusterId, String index, long startTime) throws IOException, InterruptedException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            logger.debug("deleteLastIndexStatus index: {} , startTime: {}", index, startTime);
            for (int i = 0; i < 3; i++) {
                BulkByScrollResponse response;
                if (startTime > 0) {
                    response = client.deleteByQuery(new DeleteByQueryRequest(lastIndexStatusIndex)
                                    .setQuery(new BoolQueryBuilder()
                                            .must(new MatchQueryBuilder("index", index))
                                            .must(new MatchQueryBuilder("startTime", startTime)))
                            , RequestOptions.DEFAULT);
                } else {
                    response = client.deleteByQuery(new DeleteByQueryRequest(lastIndexStatusIndex)
                                    .setQuery(new BoolQueryBuilder()
                                            .must(new MatchQueryBuilder("index", index)))
                            , RequestOptions.DEFAULT);
                }
                if (response != null && response.getDeleted() == 0) {
                    Thread.sleep(500);
                } else {
                    break;
                }
            }
        }
    }
}
