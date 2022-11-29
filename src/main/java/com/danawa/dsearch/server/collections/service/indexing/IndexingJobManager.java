package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.status.StatusService;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.notice.AlertService;
import com.danawa.dsearch.server.utils.TimeUtils;
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
    /**
     * 인덱싱하는 과정 및 결과를 관리하는 객체 입니다.
     * 외부 혹은 내부 인덱서와 통신 및 해당 Job에 대한 삭제 및 조회의 책임을 갖습니다
     * 또한, 중간 과정들을 로깅할수 있는 객체로 넘기는 책임을 갖습니다
     *
     * manageQueue는 실질적으로 indexing 되는 과정을 관리합니다
     * lookupQueue는 나중에 조회가 들어왔을 시 결과를 저장해두는 큐입니다.
     * deleteQueue는 색인이 끝난 후 manageQueue에서 해당 내용을 제거하는데 쓰이는 큐입니다.
     */

    private static final Logger logger = LoggerFactory.getLogger(IndexingJobManager.class);


    // 색인 관리 큐
    // key: collection id,  value: Indexing status
    private ConcurrentHashMap<String, IndexingStatus> manageQueue = new ConcurrentHashMap<>();
    // 디서치 인덱서가 상태 조회 할 때 쓰는 큐
    private ConcurrentHashMap<String, IndexingStatus> lookupQueue = new ConcurrentHashMap<>();
    // 색인 관리 큐에서 제거 용도 임시 저장소
    private Set<String> deleteQueue = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());


    private IndexingJobService indexingJobService;
    private StatusService indexStatusService;
    private HistoryService indexHistoryService;
    private IndexerClient indexerClient;

    private long timeout;

    public IndexingJobManager(IndexingJobService indexingJobService,
                              IndexerClient indexerClient,
                              StatusService indexStatusService,
                              HistoryService indexHistoryService,
                              @Value("${dsearch.timeout}") long timeout) {
        this.timeout = timeout;
        this.indexStatusService = indexStatusService;
        this.indexHistoryService = indexHistoryService;
        this.indexingJobService = indexingJobService;
        this.indexerClient = indexerClient;
    }

    @Scheduled(fixedDelay = 1000)
    private void manageIndexingStatus() {
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
                processOnTimeout(indexingStatus, collectionId);
            } catch (IndexingJobFailureException|IOException e) {
                logger.error("{}", e);
                processRetry(indexingStatus, collectionId);
            }
        });

        deleteInScheduleQueue(); // scheduleQueue 키 삭제는 여기서
    }

    private void processRetry(IndexingStatus indexingStatus, String collectionId){
        IndexActionStep currentStep = indexingStatus.getCurrentStep();
        String status = indexingStatus.getStatus();

        if (indexingStatus.getRetry() > 0) {
            indexingStatus.setRetry(indexingStatus.getRetry() - 1);
            logger.error("현재 재시도 중 입니다.. index:{}, count: {}", indexingStatus.getIndex(), indexingStatus.getRetry());
        } else {
            if("STOP".equalsIgnoreCase(status)){
                createErrorHistoryAndUpdateStatus(indexingStatus);
            }else if (currentStep == IndexActionStep.FULL_INDEX || currentStep == IndexActionStep.DYNAMIC_INDEX) {
                createErrorHistoryAndUpdateStatus(indexingStatus);
                indexerClient.deleteJob(indexingStatus);
                logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
            } else {
                logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
            }
            manageQueue.remove(collectionId);
        }
    }

    private void processOnTimeout(IndexingStatus indexingStatus, String collectionId){
        if (isTimeout(indexingStatus.getStartTime())){
            indexingStatus.setStatus("STOP");
            indexingStatus.setEndTime(System.currentTimeMillis());
            logger.error("index: {}, Timeout {} hours..", indexingStatus.getIndex(), (timeout / (1000 * 60 * 60)));

            indexingJobService.stopIndexing(indexingStatus);
            deleteQueue.add(collectionId);
            createErrorHistoryAndUpdateStatus(indexingStatus);
        }
    }

    private boolean isTimeout(long startTime){
        return System.currentTimeMillis() - startTime >= timeout;
    }

    private void deleteInScheduleQueue(){
        for(String collectionId: deleteQueue){
            manageQueue.remove(collectionId);
            deleteQueue.remove(collectionId);
        }
    }

    private void processExposeStep(IndexingStatus indexingStatus, String collectionId) throws IOException {
        // 인덱스 alias 교체
        changeIndexAlias(indexingStatus);
        // 인덱싱 결과 조회용 큐에 적재
        updateLookupQueue(collectionId, "SUCCESS");
        // 혹시나 있을 인덱싱 중지
        indexingJobService.stopIndexing(indexingStatus);
        // 기존 latest status 변경
        indexStatusService.update(indexingStatus, "SUCCESS");
        // 로깅
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX);
        indexHistoryService.create(indexingStatus, "SUCCESS");
        // 큐에서 삭제
        deleteQueue.add(collectionId);
    }

    private void changeIndexAlias(IndexingStatus indexingStatus) {
        UUID clusterId = indexingStatus.getClusterId();
        Collection collection = indexingStatus.getCollection();
        while(true){
            try{
                indexingJobService.expose(clusterId, collection, indexingStatus.getIndex());
            } catch (IOException e) {
                TimeUtils.sleep(1000);
                continue;
            }
            break;
        }
    }

    private void updateLookupQueue(String collectionId, String status){
        IndexingStatus indexingStatus = manageQueue.get(collectionId);
        indexingStatus.setStatus(status);
        indexingStatus.setEndTime(System.currentTimeMillis());
        lookupQueue.put(collectionId, indexingStatus);
    }

    private void createErrorHistoryAndUpdateStatus(IndexingStatus indexingStatus){
        String status = "ERROR";
        String docSize = "0";
        String store = "0";
        indexHistoryService.create(indexingStatus, docSize, status, store);
        indexStatusService.update(indexingStatus, status);
    }

    public void add(String collectionId, IndexingStatus indexingStatus) throws IndexingJobFailureException {
        IndexingStatus registerIndexingStatus = getManageQueue(collectionId);
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
        if (register) {
            indexStatusService.create(indexingStatus, "READY");
        }
        manageQueue.put(collectionId, indexingStatus);
    }

    public IndexingStatus getManageQueue(String collectionId) {
        return manageQueue.get(collectionId);
    }

    public boolean isExistInManageQueue(String collectionId){
        if(manageQueue.get(collectionId) == null) return false;
        return true;
    }

    private void processIndexStep(IndexingStatus indexingStatus, String collectionId) throws IOException{
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        Collection collection = indexingStatus.getCollection();
        long endTime = System.currentTimeMillis();

        IndexerStatus status = indexerClient.getStatus(indexingStatus);

        if (status == IndexerStatus.SUCCESS){
            indexingStatus.setEndTime(endTime);

            indexStatusService.update(indexingStatus, status.toString());
            IndexActionStep nextStep = indexingStatus.getNextStep().poll();

            logger.info("Indexing {} ==> clusterId: {}, index: {}, collectionId", status, clusterId, indexingStatus.getIndex(), collectionId);
            indexingJobService.changeRefreshInterval(clusterId, collection, index); // 색인 끝나면 refresh_interval 변경

            if (nextStep != null) {
                indexStatusService.create(indexingStatus, "RUNNING"); // Expose가 남아 있기 때문에 status 추가
                indexingStatus.setCurrentStep(nextStep); // 교체 단계 셋팅 (안하면 Null Pointer Exception)
                manageQueue.put(collectionId, indexingStatus); // 교체 단계가 있으므로 다시 스케줄에 넣는다.
            }else{
                deleteQueue.add(collectionId);
            }

            updateLookupQueue(collectionId, status.toString());
            indexerClient.deleteJob(indexingStatus);
            // refresh interval 을 수행한 뒤 60초 대기 시간 추가
            // 다큐먼트 카운트가 전부 업데이트 되기 전에 cat/indices 조회 시 진행 중인 값으로 가져옴
            TimeUtils.sleep(60000);
        }else if (status == IndexerStatus.ERROR || status == IndexerStatus.STOP){
            logger.info("Indexing {} ==> clusterId: {}, index: {}, collectionId", status, clusterId, indexingStatus.getIndex(), collectionId);
            indexingStatus.setEndTime(endTime);
            indexHistoryService.create(indexingStatus, status.toString()); // 로깅
            indexStatusService.update(indexingStatus, status.toString());

            deleteQueue.add(collectionId);
            updateLookupQueue(collectionId, status.toString());
            indexerClient.deleteJob(indexingStatus);
        } else if(status == IndexerStatus.RUNNING){
            indexingStatus.setStatus(IndexerStatus.RUNNING.toString());
            updateLookupQueue(collectionId, status.toString());
            manageQueue.put(collectionId, indexingStatus);
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
        if(lookupQueue.get(collectionId) != null){
            IndexingStatus indexingStatus = lookupQueue.get(collectionId);
            indexingStatus.setStatus(status);
            lookupQueue.replace(collectionId, indexingStatus);
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
        for (String key : lookupQueue.keySet()) {
            // key 는 collectionId(UUID) 이다
            IndexingStatus indexingStatus = lookupQueue.get(key);
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
        if(lookupQueue.get(collectionId) != null){
            return lookupQueue.get(collectionId);
        }
        return IndexingStatus.Empty;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}

