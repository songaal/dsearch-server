package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexingJobManager {
    private static final Logger logger = LoggerFactory.getLogger(IndexingJobManager.class);
    private final IndexingJobService indexingJobService;
    private final LoggingIndexHistoryService loggingIndexHistoryService;
    private final ElasticsearchFactory elasticsearchFactory;

    // KEY: collection id, value: Indexing status
    private Set<String> deleteQueue = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>()); // 색인 관리 큐에서 제거 용도 임시 저장소
    private ConcurrentHashMap<String, IndexingStatus> manageQueue = new ConcurrentHashMap<>(); // 색인 관리 큐

    private ConcurrentHashMap<String, IndexingStatus> statusLookupQueue = new ConcurrentHashMap<>(); // 디서치 인덱서가 상태 조회 할 때 쓰는 큐

    private IndexerClient indexerClient;
    private long timeout;

    public IndexingJobManager(IndexingJobService indexingJobService,
                              ElasticsearchFactory elasticsearchFactory,
                              IndexerClient indexerClient,
                              LoggingIndexHistoryService loggingIndexHistoryService,
                              @Value("${dsearch.timeout}") long timeout) {
        this.timeout = timeout;
        this.indexingJobService = indexingJobService;
        this.elasticsearchFactory = elasticsearchFactory;
        this.loggingIndexHistoryService = loggingIndexHistoryService;
        this.indexerClient = indexerClient;
    }

    @Scheduled(fixedDelay = 1000)
    private void fetchIndexingStatus() {
        if (manageQueue.size() == 0) return;

        Iterator<Map.Entry<String, IndexingStatus>> entryIterator = manageQueue.entrySet().iterator();

        entryIterator.forEachRemaining(entry -> { // key == collectionId
            String collectionId = entry.getKey();
            IndexingStatus indexingStatus = entry.getValue();
            IndexActionStep currentStep = indexingStatus.getCurrentStep();
            logger.info("clusterId={}, index={}, step={}", indexingStatus.getClusterId(), indexingStatus.getIndex(), currentStep.name());

            try {
                if (currentStep == null) {
                    deleteQueue.add(collectionId);
                    logger.error("index: {}, NOT Matched Step..", indexingStatus.getIndex());
                    throw new IndexingJobFailureException("다음 진행 내용이 없습니다.");
                } else if (currentStep == IndexActionStep.FULL_INDEX || currentStep == IndexActionStep.DYNAMIC_INDEX) {
                    processIndexStep(indexingStatus, collectionId);
                } else if (currentStep == IndexActionStep.EXPOSE) {
                    processExposeStep(indexingStatus, collectionId);
                }
                processOnTimeout(collectionId, indexingStatus);
            } catch (IndexingJobFailureException|IOException e) {
                logger.error("{}", e);
                processRetry(collectionId, indexingStatus);
            }
        });

        deleteInScheduleQueue(); // scheduleQueue 키 삭제는 여기서
    }

    private void processRetry(String collectionId, IndexingStatus indexingStatus){
        IndexActionStep currentStep = indexingStatus.getCurrentStep();
        String status = indexingStatus.getStatus();

        if (indexingStatus.getRetry() > 0) {
            indexingStatus.setRetry(indexingStatus.getRetry() - 1);
            logger.error("현재 재시도 중 입니다.. index:{}, count: {}", indexingStatus.getIndex(), indexingStatus.getRetry());
        } else {
            if("STOP".equalsIgnoreCase(status)){
                addErrorIndexHistory(indexingStatus);
            }else if (currentStep == IndexActionStep.FULL_INDEX || currentStep == IndexActionStep.DYNAMIC_INDEX) {
                addErrorIndexHistory(indexingStatus);
                indexerClient.deleteJob(indexingStatus);
                logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
            } else {
                logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
            }
            manageQueue.remove(collectionId);
        }
    }

    private void deleteInScheduleQueue(){
        for(String collectionId: deleteQueue){
            logger.info("removed: {}", collectionId);
            manageQueue.remove(collectionId);
        }
    }

    private void processExposeStep(IndexingStatus indexingStatus, String collectionId) throws IOException {
        changeAliasToNewIndex(indexingStatus); // 인덱스 교체
        changeLookupQueueStatus(collectionId, "SUCCESS"); // 인덱싱 결과 조회용 큐에 적재
        removeTempDocAndStopIndexing(collectionId); // 색인 진행 내용을 임시 저장하는 인덱스에서 데이터 삭제 및 혹시나 있을 인덱싱 중지
        deleteQueue.add(collectionId);
    }

    private void changeAliasToNewIndex(IndexingStatus indexingStatus) throws IOException {
        UUID clusterId = indexingStatus.getClusterId();
        Collection collection = indexingStatus.getCollection();
        indexingJobService.expose(clusterId, collection, indexingStatus.getIndex());
    }

    private void changeLookupQueueStatus(String collectionId, String status){
        IndexingStatus indexingStatus = manageQueue.get(collectionId);
        indexingStatus.setStatus(status);
        indexingStatus.setEndTime(System.currentTimeMillis());
        statusLookupQueue.put(collectionId, indexingStatus);
        logger.info("LookupQueue ==> clusterId: {}, collectionId: {}, indexingStatus: {}", indexingStatus.getClusterId(), collectionId, indexingStatus);
    }

    private void removeTempDocAndStopIndexing(String collectionId){
        IndexingStatus indexingStatus = manageQueue.get(collectionId);
        UUID clusterId = indexingStatus.getClusterId();

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            indexingJobService.stopIndexing(indexingStatus);
            loggingIndexHistoryService.deleteLastIndexStatus(client, indexingStatus.getIndex(), -1);
        }catch (IOException e){
            logger.error("엘라스틱서치({}:{}) 연결 실패... {}", indexingStatus.getHost(), indexingStatus.getPort(), e);
        }
    }

    private void processOnTimeout(String collectionId, IndexingStatus indexingStatus){
        if (isTimeout(indexingStatus.getStartTime())){
            indexingStatus.setStatus("STOP");
            indexingStatus.setEndTime(System.currentTimeMillis());
            logger.error("index: {}, Timeout {} hours..", indexingStatus.getIndex(), (timeout / (1000 * 60 * 60)));

            indexingJobService.stopIndexing(indexingStatus);
            deleteQueue.add(collectionId);
            addErrorIndexHistory(indexingStatus);
        }
    }

    private boolean isTimeout(long startTime){
        return System.currentTimeMillis() - startTime >= timeout;
    }

    private void addErrorIndexHistory(IndexingStatus indexingStatus){
        String status = "ERROR";
        String docSize = "0";
        String store = "0";

        try {
            loggingIndexHistoryService.addIndexHistory(indexingStatus, docSize, status, store);
        }catch (IOException e){
            logger.error("엘라스틱서치({}:{}) 연결 실패... {}", indexingStatus.getHost(), indexingStatus.getPort(), e);
        }
    }

    public void add(String collectionId, IndexingStatus indexingStatus) throws IndexingJobFailureException {
        IndexingStatus registerIndexingStatus = getScheduleQueue(collectionId);
        if (registerIndexingStatus != null) {
            throw new IndexingJobFailureException("Duplicate Collection Indexing");
        }
        if (indexingStatus.getClusterId() == null) {
            throw new IndexingJobFailureException("Cluster Id Required Field.");
        }
        if (indexingStatus.getCurrentStep() == null) {
            throw new IndexingJobFailureException("Empty Current Step");
        }

        add(collectionId, indexingStatus, true);
    }
    public void add(String collectionId, IndexingStatus indexingStatus, boolean register) throws IndexingJobFailureException {
        try {
            if (register) {
                loggingIndexHistoryService.addLastIndexStatus(indexingStatus, "READY");
            }
            manageQueue.put(collectionId, indexingStatus);
        } catch (IOException e) {
            logger.error("", e);
            throw new IndexingJobFailureException(e);
        }

//        try (RestHighLevelClient client = elasticsearchFactory.getClient(indexingStatus.getClusterId())) {
//            if (register) {
//                loggingIndexHistoryService.addLastIndexStatus(client, collectionId, indexingStatus.getIndex(), indexingStatus.getStartTime(), "READY", indexingStatus.getCurrentStep().name(), indexingStatus.getIndexingJobId());
//            }
//            scheduleQueue.put(collectionId, indexingStatus);
//        } catch (IOException e) {
//            logger.error("", e);
//            throw new IndexingJobFailureException(e);
//        }
    }

    public IndexingStatus getScheduleQueue(String collectionId) {
        return manageQueue.get(collectionId);
    }

    private void processIndexStep(IndexingStatus indexingStatus, String collectionId) throws IOException{
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        Collection collection = indexingStatus.getCollection();

        IndexerStatus status = indexerClient.getStatus(indexingStatus);

        if (status == IndexerStatus.SUCCESS){
            indexingStatus.setEndTime(System.currentTimeMillis());
            loggingIndexHistoryService.changeLatestIndexHistory(indexingStatus, status);
            IndexActionStep nextStep = indexingStatus.getNextStep().poll();

            if (nextStep != null) {
                logger.info("Indexing {} ==> clusterId: {}, index: {}, collectionId", status, clusterId, indexingStatus.getIndex(), collectionId);
                indexingJobService.changeRefreshInterval(clusterId, collection, index); // 색인 끝나면 refresh_interval 변경
                loggingIndexHistoryService.addLastIndexStatus(indexingStatus,"RUNNING"); // 성공 로그 남기기
                indexingStatus.setCurrentStep(nextStep); // 다음 단계 셋팅 (안하면 Null Pointer Exception)
                manageQueue.put(collectionId, indexingStatus); // 다음단계가 있으므로 다시 스케줄에 넣는다.
            }else{
                deleteQueue.add(collectionId);
            }

            changeLookupQueueStatus(collectionId, status.toString());
            indexerClient.deleteJob(indexingStatus);
        }else if (status == IndexerStatus.ERROR || status == IndexerStatus.STOP){
            logger.info("Indexing {} ==> clusterId: {}, index: {}, collectionId", status, clusterId, indexingStatus.getIndex(), collectionId);

            loggingIndexHistoryService.changeLatestIndexHistory(indexingStatus, status);
            indexingStatus.setEndTime(System.currentTimeMillis());
            changeLookupQueueStatus(collectionId, status.toString());
            deleteQueue.add(collectionId);
            indexerClient.deleteJob(indexingStatus);
        } else if(status == IndexerStatus.RUNNING){
            indexingStatus.setStatus(IndexerStatus.RUNNING.toString());
            manageQueue.put(collectionId, indexingStatus);
            changeLookupQueueStatus(collectionId, status.toString());
        } else { // UNKNOWN
            deleteQueue.add(collectionId);
        }
    }
    public Map<String, Object> getCurrentIndexingList(UUID clusterId){
        Map<String, Object> map = new HashMap<>();
        for(String key : manageQueue.keySet()) {
            if (clusterId.equals(manageQueue.get(key).getClusterId())) {
                Map<String, Object> template = new HashMap<>();
                template.put("server", manageQueue.get(key));
                map.put(key, template);
            }
        }
        return map;
    }

    public void setQueueStatus(String collectionId, String status){
        setScheduleQueueStatus(collectionId, status);
        setLookupQueueStatus(collectionId, status);
    }

    private void setLookupQueueStatus(String collectionId, String status){
        if(statusLookupQueue.get(collectionId) != null){
            IndexingStatus indexingStatus = statusLookupQueue.get(collectionId);
            indexingStatus.setStatus(status);
            statusLookupQueue.replace(collectionId, indexingStatus);
        }
    }

    private void setScheduleQueueStatus(String collectionId, String status){
        if(manageQueue.get(collectionId) != null){
            IndexingStatus currentStatus = manageQueue.get(collectionId);
            currentStatus.setStatus(status);
            manageQueue.replace(collectionId, currentStatus);
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

    public List<IndexingStatus> getManageQueueList(){
        List<IndexingStatus> result = new ArrayList<>();
        for (String key : manageQueue.keySet()) {
            // key 는 collectionId(UUID) 이다
            IndexingStatus indexingStatus = manageQueue.get(key);
            result.add(indexingStatus);
        }

        return result;
    }

    public IndexingStatus getCurrentIndexingStatus(String collectionId){
        if(manageQueue.get(collectionId) != null){
            return manageQueue.get(collectionId);
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

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}

