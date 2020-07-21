package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.IndexStep;
import com.danawa.fastcatx.server.entity.IndexingStatus;
import com.danawa.fastcatx.server.excpetions.IndexingJobFailureException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexingJobManager {
    private static Logger logger = LoggerFactory.getLogger(IndexingJobManager.class);
    private static RestTemplate restTemplate;

    private final IndexingJobService indexingJobService;
    private final ElasticsearchFactory elasticsearchFactory;

    private final String lastIndexStatusIndex = ".fastcatx_last_index_status";
    private final String indexHistory = ".fastcatx_index_history";

    // KEY: collection id, value: Indexing status
    private ConcurrentHashMap<String, IndexingStatus> jobs = new ConcurrentHashMap<>();

    public IndexingJobManager(IndexingJobService indexingJobService, ElasticsearchFactory elasticsearchFactory) {
        this.indexingJobService = indexingJobService;
        this.elasticsearchFactory = elasticsearchFactory;
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    @Scheduled(cron = "*/5 * * * * *")
    private void fetchIndexingStatus() {
        if (jobs.size() == 0) {
            return;
        }
        Iterator<Map.Entry<String, IndexingStatus>> entryIterator = jobs.entrySet().iterator();
        entryIterator.forEachRemaining(entry -> {
            // key == collectionId
            String id = entry.getKey();
            IndexingStatus indexingStatus = entry.getValue();
            IndexStep step = indexingStatus.getCurrentStep();
            try {
                if (step == null) {
                    throw new Exception("Invalid Step");
                } else if (step == IndexStep.INDEX) {
                    updateIndexerStatus(id, indexingStatus);
                } else if (step == IndexStep.PROPAGATE) {
                    updateRecoveryStatus(id, indexingStatus);
                } else {
                    logger.error("index: {}, NOT Matched Step.. {}", indexingStatus.getIndex(), step);
                }
            } catch (Exception e) {
                if (indexingStatus.getRetry() > 0) {
                    indexingStatus.setRetry(indexingStatus.getRetry() - 1);
                } else {
//                    TODO retry 하였지만 에러가 발생시.. 히스토리 추가 후 잡에서 제거
                    jobs.remove(id);
                    logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
                }
            }
        });
    }

    public void add(String collectionId, IndexingStatus indexingStatus) throws IndexingJobFailureException {
        IndexingStatus registerIndexingStatus = findById(collectionId);
        if (indexingStatus.getClusterId() == null) {
            throw new IndexingJobFailureException("Cluster Id Required Field.");
        }
        if (registerIndexingStatus != null) {
            throw new IndexingJobFailureException("Duplicate Collection Indexing");
        }
        jobs.put(collectionId, indexingStatus);
    }

    public IndexingStatus findById(String collectionId) {
        return jobs.get(collectionId);
    }

    /**
     * indexer 조회 후 상태 업데이트.
     * */
    private void updateIndexerStatus(String id, IndexingStatus indexingStatus) {
        URI url = URI.create(String.format("http://%s:%d/async/status?id=%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
        String status = (String) responseEntity.getBody().get("status");
        if ("SUCCESS".equalsIgnoreCase(status)) {
            IndexStep nextStep = indexingStatus.getNextStep().poll();
            if (nextStep != null) {
                logger.debug("next Step >> {}", nextStep);
                indexingStatus.setCurrentStep(nextStep);
            } else {
                logger.debug("empty next Step >> {}", id);
                // 다음 작업이 없으면 제거.
                jobs.remove(id);
            }
        } else if ("ERROR".equalsIgnoreCase(status)) {
            // TODO index_history 내역 추가.
            jobs.remove(id);
        }
    }

    /**
     * elasticsearch 조회 후 상태 업데이트.
     * */
    private void updateRecoveryStatus(String id, IndexingStatus indexingStatus) {
        // 완료 여부만 체크함.

    }

    public void addIndexHistory(RestHighLevelClient client, String index, String jobType, long startTime, long endTime, boolean autoRun, String docSize, String status, String store) {
        try {
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
        } catch(Exception e) {
            logger.error("addIndexHistory >> index: {}", index, e);
        }
    }

    public void addLastIndexStatus(RestHighLevelClient client, String index, long startTime, String status) {
        try {
            Map<String, Object> source = new HashMap<>();
            source.put("index", index);
            source.put("startTime", startTime);
            source.put("status", status);
            client.index(new IndexRequest().index(lastIndexStatusIndex).source(source), RequestOptions.DEFAULT);
        } catch(Exception e) {
            logger.error("addLastIndexStatus >> index: {}", index, e);
        }
    }
    public void deleteLastIndexStatus(RestHighLevelClient client, String index, long startTime) {
        try {
            client.deleteByQuery(new DeleteByQueryRequest(lastIndexStatusIndex)
                            .setQuery(new BoolQueryBuilder()
                                    .must(new MatchQueryBuilder("index", index))
                                    .must(new MatchQueryBuilder("startTime", startTime)))
                    , RequestOptions.DEFAULT);
        } catch(Exception e) {
            logger.error("deleteLastIndexStatus >> index: {}", index, e);
        }
    }


}
