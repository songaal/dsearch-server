package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.notice.NoticeHandler;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class LoggingIndexHistoryService {
    private static final Logger logger = LoggerFactory.getLogger(LoggingIndexHistoryService.class);

    private final String lastIndexStatusIndex = ".dsearch_last_index_status";

    private final String indexHistory = ".dsearch_index_history";

    private final ElasticsearchFactory elasticsearchFactory;

    public LoggingIndexHistoryService(ElasticsearchFactory elasticsearchFactory){
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void addLastIndexStatus(IndexingStatus status, String currentStatus) throws IOException {
        UUID clusterId = status.getClusterId();
        String collectionId = status.getCollection().getId();
        long startTime = status.getStartTime();
        String index = status.getIndex();
        String step = status.getCurrentStep().name();
        String jobId = status.getIndexingJobId();
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            addLastIndexStatus(client, collectionId, index, startTime, currentStatus, step, jobId);
        }
    }
    private void addLastIndexStatus(RestHighLevelClient client, String collectionId, String index, long startTime, String status, String step, String jobId) throws IOException {
        Map<String, Object> source = new HashMap<>();
        source.put("collectionId", collectionId);
        source.put("index", index);
        source.put("startTime", startTime);
        source.put("status", status);
        source.put("step", step);
        source.put("jobId", jobId);
        client.index(new IndexRequest().index(lastIndexStatusIndex).source(source), RequestOptions.DEFAULT);
    }

    public void addIndexHistory(IndexingStatus indexingStatus, String docSize, String status, String store) throws IOException {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        String jobType = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        long endTime = indexingStatus.getEndTime();
        boolean autoRun = indexingStatus.isAutoRun();

        noticeIfIndexingError(index, status, jobType);

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            deleteLastIndexStatus(client, index, startTime);

            // 이력의 갯수를 체크하여 0일때만 이력에 남김.
            BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
            countQuery.filter().add(QueryBuilders.termQuery("index", index));
            countQuery.filter().add(QueryBuilders.termQuery("startTime", startTime));
            countQuery.filter().add(QueryBuilders.termQuery("jobType", jobType));

            CountResponse countResponse = client.count(new CountRequest(indexHistory).query(countQuery), RequestOptions.DEFAULT);
            logger.debug("addIndexHistory: index: {}, startTime: {}, jobType: {}, result Count: {}", index, startTime, jobType, countResponse.getCount());
            if (countResponse.getCount() == 0) {
                Map<String, Object> source = new HashMap<>();
                source.put("index", index);
                source.put("jobType", jobType);
                source.put("startTime", startTime);
                source.put("endTime", endTime);
                source.put("autoRun", autoRun);
                source.put("status", status);
                source.put("docSize", docSize);
                source.put("store", store);
                client.index(new IndexRequest().index(indexHistory).source(source), RequestOptions.DEFAULT);
            }
            deleteLastIndexStatus(client, index, startTime);
        }

    }

    private void noticeIfIndexingError(String index, String status, String jobType){
        if ("ERROR".equalsIgnoreCase(status)) {
            if ("FULL_INDEX".equalsIgnoreCase(jobType) || "DYNAMIC_INDEX".equalsIgnoreCase(jobType)) {
                NoticeHandler.send(String.format("%s 인덱스의 색인이 실패하였습니다.", index));
            } else if ("PROPAGATE".equalsIgnoreCase(jobType)) {
                NoticeHandler.send(String.format("%s 인덱스의 전파가 실패하였습니다.", index));
            } else {
                NoticeHandler.send(String.format("%s 인덱스의 %s가 실패하였습니다.", index, jobType));
            }
        }
    }

    public void changeLatestIndexHistory(IndexingStatus indexingStatus, IndexerStatus status) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();

        while(true){
            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                client.getLowLevelClient().performRequest(new Request("POST", String.format("/%s/_flush", index)));

                Map<String, Object> catIndex = catIndex(client, index);
                String docSize = (String) catIndex.get("docs.count");
                String store = (String) catIndex.get("store.size");
                long startTime = indexingStatus.getStartTime();
                deleteLastIndexStatus(client, index, startTime);
                addIndexHistory(indexingStatus, docSize, status.toString(), store);
                break;
            } catch (IOException e) {
                logger.error("{}", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }
        }
    }

    private Map<String ,Object> catIndex(RestHighLevelClient client, String index) throws IOException {
        Request request = new Request("GET", String.format("/_cat/indices/%s", index));
        request.addParameter("format", "json");
        Response response = client.getLowLevelClient().performRequest(request);
        String responseBodyString = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
        return catIndices == null && catIndices.size() > 0 ? new HashMap<>() : catIndices.get(0);
    }

    public void deleteLastIndexStatus(RestHighLevelClient client, String index, long startTime) {
        try {
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
        } catch (IOException e) {
            logger.error("deleteLastIndexStatus >> index: {}", index, e);
        } catch (InterruptedException e) {
            logger.error("deleteLastIndexStatus >> index: {}", index, e);
        }
    }
}
