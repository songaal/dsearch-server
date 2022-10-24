package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.notice.NoticeHandler;
import com.danawa.fastcatx.indexer.IndexJobManager;
import com.danawa.fastcatx.indexer.entity.Job;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexingJobManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexingJobManager.class);
    private static RestTemplate restTemplate;
    private final IndexingJobService indexingJobService;
    private final ElasticsearchFactory elasticsearchFactory;
    private final IndexJobManager indexerJobManager;

    private final String lastIndexStatusIndex = ".dsearch_last_index_status";
    private final String indexHistory = ".dsearch_index_history";

    // KEY: collection id, value: Indexing status
    private ConcurrentHashMap<String, IndexingStatus> scheduleQueue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, IndexingStatus> statusLookupQueue = new ConcurrentHashMap<>();
    private long timeout;

    public IndexingJobManager(IndexingJobService indexingJobService,
                              ElasticsearchFactory elasticsearchFactory,
                              IndexJobManager indexerJobManager,
                              @Value("${dsearch.timeout}") long timeout) {
        this.indexerJobManager = indexerJobManager;
        this.timeout = timeout;
        this.indexingJobService = indexingJobService;
        this.elasticsearchFactory = elasticsearchFactory;
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    @Scheduled(fixedDelay = 1000)
    private void fetchIndexingStatus() {
        if (scheduleQueue.size() == 0) return;

        Iterator<Map.Entry<String, IndexingStatus>> entryIterator = scheduleQueue.entrySet().iterator();

        entryIterator.forEachRemaining(entry -> { // key == collectionId
            String collectionId = entry.getKey();
            IndexingStatus indexingStatus = entry.getValue();
            IndexActionStep currentStep = indexingStatus.getCurrentStep();
            String status = indexingStatus.getStatus();
            String index = indexingStatus.getIndex();
            UUID clusterId = indexingStatus.getClusterId();
            logger.info("cluster={}, index={}, step={}", clusterId, index, currentStep.name());

            try {
                if (currentStep == null) {
                    scheduleQueue.remove(collectionId); // 스케쥴 안에서 삭제만 진행
                    logger.error("index: {}, NOT Matched Step..", index);
                    throw new IndexingJobFailureException("다음 진행 내용이 없습니다.");
                } else if (currentStep == IndexActionStep.FULL_INDEX || currentStep == IndexActionStep.DYNAMIC_INDEX) {
                    processIndexStep(indexingStatus, collectionId);
                } else if (currentStep == IndexActionStep.EXPOSE) {
                    processExposeStep(indexingStatus, collectionId);
                }
                saveErrorHistoryIfTimeout(collectionId, indexingStatus);
            } catch (IndexingJobFailureException|IOException e) {
                // 재시도
                if (indexingStatus.getRetry() > 0) {
                    indexingStatus.setRetry(indexingStatus.getRetry() - 1);
                    logger.error("현재 재시도 중 입니다.. index:{}, count: {}", indexingStatus.getIndex(), indexingStatus.getRetry());
                } else {
                    if("STOP".equalsIgnoreCase(status)){
                        saveErrorHistory(clusterId, indexingStatus);
                    }else if (currentStep == IndexActionStep.FULL_INDEX || currentStep == IndexActionStep.DYNAMIC_INDEX) {
                        saveErrorHistory(clusterId, indexingStatus);
                        sendDeleteJobRequest(indexingStatus);
                        logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
                    } else {
                        logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
                    }
                    scheduleQueue.remove(collectionId);
                }
                logger.error("{}", e);
            }
        });
    }

    private void processExposeStep(IndexingStatus indexingStatus, String collectionId) throws IOException {
        changeAliasToNewIndex(indexingStatus); // 인덱스 교체
        changeLookupQueueStatus(collectionId, "SUCCESS"); // 인덱싱 결과 조회용 큐에 적재
        removeTempDocAndStopIndexing(collectionId); // 색인 진행 내용을 임시 저장하는 인덱스에서 데이터 삭제 및 혹시나 있을 인덱싱 중지
        scheduleQueue.remove(collectionId); // 정상적으로 실행되었다면 스케쥴 큐에서 삭제
    }

    private void changeAliasToNewIndex(IndexingStatus indexingStatus) throws IOException {
        UUID clusterId = indexingStatus.getClusterId();
        Collection collection = indexingStatus.getCollection();
        indexingJobService.expose(clusterId, collection, indexingStatus.getIndex());
    }

    private void changeLookupQueueStatus(String collectionId, String status){
        IndexingStatus indexingStatus = scheduleQueue.get(collectionId);
        indexingStatus.setStatus(status);
        indexingStatus.setEndTime(System.currentTimeMillis());
        statusLookupQueue.put(collectionId, indexingStatus);
        logger.info("인덱스 교체 성공 ==> clusterId: {}, collectionId: {}, indexingStatus: {}", indexingStatus.getClusterId(), collectionId, indexingStatus);
    }

    private void removeTempDocAndStopIndexing(String collectionId){
        IndexingStatus indexingStatus = scheduleQueue.get(collectionId);
        UUID clusterId = indexingStatus.getClusterId();

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            indexingJobService.stopIndexing(indexingStatus.getScheme(), indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId());
            deleteLastIndexStatus(client, indexingStatus.getIndex(), -1);
        }catch (IOException e){
            logger.error("엘라스틱서치({}:{}) 연결 실패... {}", indexingStatus.getHost(), indexingStatus.getPort(), e);
        }
    }

    private void saveErrorHistoryIfTimeout(String collectionId, IndexingStatus indexingStatus){
        if (isTimeout(indexingStatus.getStartTime())){
            UUID clusterId = indexingStatus.getClusterId();
            indexingStatus.setStatus("STOP");
            indexingStatus.setEndTime(System.currentTimeMillis());
            logger.error("index: {}, Timeout {} hours..", indexingStatus.getIndex(), (timeout / (1000 * 60 * 60)));

            saveErrorHistory(clusterId, indexingStatus);
            scheduleQueue.remove(collectionId);
        }
    }

    private boolean isTimeout(long startTime){
        return System.currentTimeMillis() - startTime >= timeout;
    }

    private void saveErrorHistory(UUID clusterId, IndexingStatus indexingStatus){
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            indexingJobService.stopIndexing(indexingStatus.getScheme(), indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId());
            deleteLastIndexStatus(client, indexingStatus.getIndex(), indexingStatus.getStartTime());
            addIndexHistory(client, indexingStatus.getIndex(),  indexingStatus.getCurrentStep().name(), indexingStatus.getStartTime(), indexingStatus.getEndTime(), indexingStatus.isAutoRun(), "0", "ERROR", "0");
        }catch (IOException e){
            logger.error("엘라스틱서치({}:{}) 연결 실패... {}", indexingStatus.getHost(), indexingStatus.getPort(), e);
        }
    }

    private void sendDeleteJobRequest(IndexingStatus indexingStatus){
        if (indexingStatus.getCollection().isExtIndexer()) {
            URI deleteUrl = URI.create(String.format("http://%s:%d/async/%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
            restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), String.class);
        } else {
            indexerJobManager.remove(UUID.fromString(indexingStatus.getIndexingJobId()));
        }
    }

    public void add(String collectionId, IndexingStatus indexingStatus) throws IndexingJobFailureException {
        add(collectionId, indexingStatus, true);
    }
    public void add(String collectionId, IndexingStatus indexingStatus, boolean register) throws IndexingJobFailureException {
        IndexingStatus registerIndexingStatus = getScheduleQueue(collectionId);
        if (indexingStatus.getClusterId() == null) {
            throw new IndexingJobFailureException("Cluster Id Required Field.");
        }
        if (registerIndexingStatus != null) {
            throw new IndexingJobFailureException("Duplicate Collection Indexing");
        }
        if (indexingStatus.getCurrentStep() == null) {
            throw new IndexingJobFailureException("Empty Current Step");
        }

        try (RestHighLevelClient client = elasticsearchFactory.getClient(indexingStatus.getClusterId())) {
            if (register) {
                addLastIndexStatus(client, collectionId, indexingStatus.getIndex(), indexingStatus.getStartTime(), "READY", indexingStatus.getCurrentStep().name(), indexingStatus.getIndexingJobId());
            }
            scheduleQueue.put(collectionId, indexingStatus);
        } catch (IOException e) {
            logger.error("", e);
            throw new IndexingJobFailureException(e);
        }
    }

    public IndexingStatus remove(String collectionId) {
        return scheduleQueue.remove(collectionId);
    }

    public IndexingStatus getScheduleQueue(String collectionId) {
        return scheduleQueue.get(collectionId);
    }

    private void processIndexStep(IndexingStatus indexingStatus, String collectionId) throws IOException{
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        int retryCount = indexingStatus.getRetry();
        String currentStep = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        String jobId = indexingStatus.getIndexingJobId();
        Collection collection = indexingStatus.getCollection();

        IndexerStatus status = getIndexerStatus(indexingStatus);

        if (status == IndexerStatus.SUCCESS){
            sendDeleteJobToIndexer(indexingStatus);
            changeLatestIndexHistory(clusterId, index, indexingStatus, status);

            IndexActionStep nextStep = indexingStatus.getNextStep().poll();
            if (nextStep != null) {
                logger.info("clusterId: {}, index: {}, collectionId: {}, indexerStatus: {}, retryCount: {}", clusterId, index, collectionId, status, retryCount);
                indexingJobService.changeRefreshInterval(clusterId, collection, index); // 색인 끝나면 refresh_interval 변경
                addLastIndexStatus(clusterId, collectionId, index, startTime,"RUNNING", currentStep, jobId ); // 성공 로그 남기기
                indexingStatus.setCurrentStep(nextStep); // 다음 단계 셋팅 (안하면 Null Pointer Exception)
                scheduleQueue.put(collectionId, indexingStatus); // 다음단계가 있으므로 다시 스케줄에 넣는다.
                logger.debug("next Step >> {}", nextStep);
            }else{
                scheduleQueue.remove(collectionId);
                logger.debug("empty next status : {} Step >> {}", status, collectionId);
            }

            changeLookupQueueStatus(collectionId, status.toString());
        }else if (status == IndexerStatus.ERROR || status == IndexerStatus.STOP){
            sendDeleteJobToIndexer(indexingStatus);
            changeLatestIndexHistory(clusterId, index, indexingStatus, status);
            logger.info("Indexing {}, id: {}, indexingStatus: {}", status, collectionId, indexingStatus);
            scheduleQueue.remove(collectionId);
        } else if(status == IndexerStatus.RUNNING){
            indexingStatus.setStatus(IndexerStatus.RUNNING.toString());
            scheduleQueue.put(collectionId, indexingStatus);
        } else { // UNKNOWN
            scheduleQueue.remove(collectionId);
        }
    }


    private IndexerStatus getIndexerStatus(IndexingStatus indexingStatus){
        boolean isExtIndexer = indexingStatus.getCollection().isExtIndexer();
        String scheme = indexingStatus.getScheme();
        String host = indexingStatus.getHost();
        int port = indexingStatus.getPort();
        String jobId = indexingStatus.getIndexingJobId();

        if (isExtIndexer) {
            URI url = URI.create(String.format("%s://%s:%d/async/status?id=%s", scheme, host, port, jobId));
            ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
            Map<String, Object> body = responseEntity.getBody();
            logger.info("external indexer status response: {}", body);

            // Null pointer Exception
            if(body.get("status") != null) return IndexerStatus.changeToStatus((String) body.get("status"));
        } else {
            Job job = indexerJobManager.status(UUID.fromString(jobId));
            logger.info("internal indexer status response: {}", job.getStatus());
            if (job != null) return IndexerStatus.changeToStatus(job.getStatus());
        }

        return IndexerStatus.UNKNOWN;
    }

    private void changeLatestIndexHistory(UUID clusterId, String index, IndexingStatus indexingStatus, IndexerStatus status) throws IOException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            client.getLowLevelClient().performRequest(new Request("POST", String.format("/%s/_flush", index)));

            Map<String, Object> catIndex = catIndex(client, index);
            String docSize = (String) catIndex.get("docs.count");
            String store = (String) catIndex.get("store.size");
            IndexActionStep step = indexingStatus.getCurrentStep();
            long startTime = indexingStatus.getStartTime();
            long endTime = System.currentTimeMillis();
            deleteLastIndexStatus(client, index, startTime);
            addIndexHistory(client, index, step.name(), startTime, endTime, indexingStatus.isAutoRun(), docSize, status.toString(), store);
        }
    }

    private void sendDeleteJobToIndexer(IndexingStatus indexingStatus){
        boolean isExtIndexer = indexingStatus.getCollection().isExtIndexer();
        String jobId = indexingStatus.getIndexingJobId();
        String scheme = indexingStatus.getScheme();
        String host = indexingStatus.getHost();
        int port = indexingStatus.getPort();

        if (isExtIndexer) {
            URI deleteUrl = URI.create(String.format("%s://%s:%d/async/%s", scheme, host, port, jobId));
            restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), String.class);
        } else {
            indexerJobManager.remove(UUID.fromString(jobId));
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

    public void addIndexHistory(RestHighLevelClient client, String index, String jobType, long startTime, long endTime, boolean autoRun, String docSize, String status, String store) {

        if ("ERROR".equalsIgnoreCase(status)) {
            if ("FULL_INDEX".equalsIgnoreCase(jobType) || "DYNAMIC_INDEX".equalsIgnoreCase(jobType)) {
                NoticeHandler.send(String.format("%s 인덱스의 색인이 실패하였습니다.", index));
            } else if ("PROPAGATE".equalsIgnoreCase(jobType)) {
                NoticeHandler.send(String.format("%s 인덱스의 전파가 실패하였습니다.", index));
            } else {
                NoticeHandler.send(String.format("%s 인덱스의 %s가 실패하였습니다.", index, jobType));
            }
        }

        try {
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
        } catch(Exception e) {
            logger.error("addIndexHistory >> index: {}", index, e);
        }
    }

    public void addLastIndexStatus(UUID clusterId, String collectionId, String index, long startTime, String status, String step, String jobId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            addLastIndexStatus(client, collectionId, index, startTime, status, step, jobId);
        }
    }
    public void addLastIndexStatus(RestHighLevelClient client, String collectionId, String index, long startTime, String status, String step, String jobId) {
        try {
            Map<String, Object> source = new HashMap<>();
            source.put("collectionId", collectionId);
            source.put("index", index);
            source.put("startTime", startTime);
            source.put("status", status);
            source.put("step", step);
            source.put("jobId", jobId);
            client.index(new IndexRequest().index(lastIndexStatusIndex).source(source), RequestOptions.DEFAULT);
        } catch(Exception e) {
            logger.error("addLastIndexStatus >> index: {}", index, e);
        }
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

        } catch(Exception e) {
            logger.error("deleteLastIndexStatus >> index: {}", index, e);
        }
    }

    public Map<String, Object> getIndexingStatusList(UUID clusterId){
        Map<String, Object> map = new HashMap<>();
        for(String key : scheduleQueue.keySet()) {
            if (clusterId.equals(scheduleQueue.get(key).getClusterId())) {
                Map<String, Object> template = new HashMap<>();
                template.put("server", scheduleQueue.get(key));
                map.put(key, template);
            }
        }
        return map;
    }

    public void setQueueStatus(String collectionId, String status){
        setScheduleQueueStatus(collectionId, status);
        setLookupQueueStatus(collectionId, status);
    }

    public void setLookupQueueStatus(String collectionId, String status){
        if(statusLookupQueue.get(collectionId) != null){
            IndexingStatus indexingStatus = statusLookupQueue.get(collectionId);
            indexingStatus.setStatus(status);
            statusLookupQueue.replace(collectionId, indexingStatus);
        }
    }
    public void setScheduleQueueStatus(String collectionId, String status){
        if(scheduleQueue.get(collectionId) != null){
            IndexingStatus currentStatus = scheduleQueue.get(collectionId);
            currentStatus.setStatus(status);
            scheduleQueue.replace(collectionId, currentStatus);
        }
    }

    public List<IndexingStatus> getLookupQueueList(){
        List<IndexingStatus> result = new ArrayList<>();
        for (String key : statusLookupQueue.keySet()) {
            // key 는 collectionId(UUID) 이다
            IndexingStatus indexingStatus = statusLookupQueue.get(key);
            result.add(indexingStatus);
        }

        return result;
    }

    public List<IndexingStatus> getScheduleQueueList(){
        List<IndexingStatus> result = new ArrayList<>();
        for (String key : scheduleQueue.keySet()) {
            // key 는 collectionId(UUID) 이다
            IndexingStatus indexingStatus = scheduleQueue.get(key);
            result.add(indexingStatus);
        }

        return result;
    }

    public IndexingStatus getIndexingStatus(String collectionId){

        if(scheduleQueue.get(collectionId) != null){
            return scheduleQueue.get(collectionId);

//            IndexingStatus currentStatus = jobs.get(collectionId);

//            idxStatus.setAction(currentStatus.getAction());
//            idxStatus.setClusterId(currentStatus.getClusterId());
//            idxStatus.setCurrentStep(currentStatus.getCurrentStep());
//            idxStatus.setCollection(currentStatus.getCollection());
//            idxStatus.setEndTime(currentStatus.getEndTime());
//            idxStatus.setError(currentStatus.getError());
//            idxStatus.setHost(currentStatus.getHost());
//            idxStatus.setIndexingJobId(currentStatus.getIndexingJobId());
//            idxStatus.setNextStep(currentStatus.getNextStep());
//            idxStatus.setPort(currentStatus.getPort());
//            idxStatus.setStartTime(currentStatus.getStartTime());

//            IndexingStatus idxStatus = new IndexingStatus();
//            idxStatus.setStatus("RUNNING");
//            return idxStatus;
        }

        if(statusLookupQueue.get(collectionId) != null){
            return statusLookupQueue.get(collectionId);
        }

        return null;
    }

    public Map<String, Object> getSettings(){
        Map<String, Object> settings = new HashMap<>();
        settings.put("indexing", this.indexingJobService.getIndexingSettings());
        return settings;
    }

    public void setSettings(String type, Map<String, Object> settings){
        this.indexingJobService.setIndexingSettings(settings);
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}

