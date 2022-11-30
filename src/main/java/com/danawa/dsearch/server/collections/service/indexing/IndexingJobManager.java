package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.IndexerStatus;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexingStep;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;
import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.status.StatusService;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
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
    private ConcurrentHashMap<String, IndexingInfo> manageQueue = new ConcurrentHashMap<>();
    // 디서치 인덱서가 상태 조회 할 때 쓰는 큐
    private ConcurrentHashMap<String, IndexingInfo> lookupQueue = new ConcurrentHashMap<>();
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

        Iterator<Map.Entry<String, IndexingInfo>> entryIterator = manageQueue.entrySet().iterator();

        entryIterator.forEachRemaining(entry -> { // key == collectionId
            String collectionId = entry.getKey();
            IndexingInfo indexingInfo = entry.getValue();
            IndexingStep currentStep = indexingInfo.getCurrentStep();
            logger.info("clusterId={}, index={}, step={}", indexingInfo.getClusterId(), indexingInfo.getIndex(), currentStep.name());

            try {
                if (currentStep == null) {
                    deleteQueue.add(collectionId);
                    logger.error("index: {}, NOT Matched Step..", indexingInfo.getIndex());
                    throw new IndexingJobFailureException("다음 진행 내용이 없습니다.");
                } else if (currentStep == IndexingStep.FULL_INDEX || currentStep == IndexingStep.DYNAMIC_INDEX) {
                    processIndexStep(indexingInfo, collectionId);
                } else if (currentStep == IndexingStep.EXPOSE) {
                    processExposeStep(indexingInfo, collectionId);
                }
                processOnTimeout(indexingInfo, collectionId);
            } catch (IndexingJobFailureException|IOException e) {
                logger.error("{}", e);
                processRetry(indexingInfo, collectionId);
            }
        });

        deleteInScheduleQueue(); // scheduleQueue 키 삭제는 여기서
    }

    private void processRetry(IndexingInfo indexingInfo, String collectionId){
        IndexingStep currentStep = indexingInfo.getCurrentStep();
        String status = indexingInfo.getStatus();

        if (indexingInfo.getRetry() > 0) {
            indexingInfo.setRetry(indexingInfo.getRetry() - 1);
            logger.error("현재 재시도 중 입니다.. index:{}, count: {}", indexingInfo.getIndex(), indexingInfo.getRetry());
        } else {
            if("STOP".equalsIgnoreCase(status)){
                createErrorHistoryAndUpdateStatus(indexingInfo);
            }else if (currentStep == IndexingStep.FULL_INDEX || currentStep == IndexingStep.DYNAMIC_INDEX) {
                createErrorHistoryAndUpdateStatus(indexingInfo);
                indexerClient.deleteJob(indexingInfo);
                logger.error("[remove job] retry.. {}", indexingInfo.getRetry());
            } else {
                logger.error("[remove job] retry.. {}", indexingInfo.getRetry());
            }
            manageQueue.remove(collectionId);
        }
    }

    private void processOnTimeout(IndexingInfo indexingInfo, String collectionId){
        if (isTimeout(indexingInfo.getStartTime())){
            indexingInfo.setStatus("STOP");
            indexingInfo.setEndTime(System.currentTimeMillis());
            logger.error("index: {}, Timeout {} hours..", indexingInfo.getIndex(), (timeout / (1000 * 60 * 60)));

            indexingJobService.stopIndexing(indexingInfo);
            deleteQueue.add(collectionId);
            createErrorHistoryAndUpdateStatus(indexingInfo);
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

    private void processExposeStep(IndexingInfo indexingInfo, String collectionId) throws IOException {
        // 인덱스 alias 교체
        changeIndexAlias(indexingInfo);
        // 인덱싱 결과 조회용 큐에 적재
        updateLookupQueue(collectionId, "SUCCESS");
        // 혹시나 있을 인덱싱 중지
        indexingJobService.stopIndexing(indexingInfo);
        // 기존 latest status 변경
        indexStatusService.update(indexingInfo, "SUCCESS");
        // 로깅
        indexingInfo.setCurrentStep(IndexingStep.FULL_INDEX);
        indexHistoryService.create(indexingInfo, "SUCCESS");
        // 큐에서 삭제
        deleteQueue.add(collectionId);
    }

    private void changeIndexAlias(IndexingInfo indexingInfo) {
        UUID clusterId = indexingInfo.getClusterId();
        Collection collection = indexingInfo.getCollection();
        while(true){
            try{
                indexingJobService.expose(clusterId, collection, indexingInfo.getIndex());
            } catch (IOException e) {
                TimeUtils.sleep(1000);
                continue;
            }
            break;
        }
    }

    private void updateLookupQueue(String collectionId, String status){
        IndexingInfo indexingInfo = manageQueue.get(collectionId);
        indexingInfo.setStatus(status);
        indexingInfo.setEndTime(System.currentTimeMillis());
        lookupQueue.put(collectionId, indexingInfo);
    }

    private void createErrorHistoryAndUpdateStatus(IndexingInfo indexingInfo){
        String status = "ERROR";
        String docSize = "0";
        String store = "0";
        indexHistoryService.create(indexingInfo, docSize, status, store);
        indexStatusService.update(indexingInfo, status);
    }

    public void add(String collectionId, IndexingInfo indexingInfo) throws IndexingJobFailureException {
        IndexingInfo registerIndexingInfo = getManageQueue(collectionId);
        if (registerIndexingInfo != null) {
            throw new IndexingJobFailureException("Duplicate Collection Indexing");
        }
        if (indexingInfo.getClusterId() == null) {
            throw new IndexingJobFailureException("Cluster Id Required Field.");
        }
        if (indexingInfo.getCurrentStep() == null) {
            throw new IndexingJobFailureException("Empty Current Step");
        }

        add(collectionId, indexingInfo, true);
    }

    public void add(String collectionId, IndexingInfo indexingInfo, boolean register) throws IndexingJobFailureException {
        if (register) {
            indexStatusService.create(indexingInfo, "READY");
        }
        manageQueue.put(collectionId, indexingInfo);
    }

    public IndexingInfo getManageQueue(String collectionId) {
        return manageQueue.get(collectionId);
    }

    public boolean isExistInManageQueue(String collectionId){
        if(manageQueue.get(collectionId) == null) return false;
        return true;
    }

    private void processIndexStep(IndexingInfo indexingInfo, String collectionId) throws IOException{
        UUID clusterId = indexingInfo.getClusterId();
        String index = indexingInfo.getIndex();
        Collection collection = indexingInfo.getCollection();
        long endTime = System.currentTimeMillis();

        IndexerStatus status = indexerClient.getStatus(indexingInfo);

        if (status == IndexerStatus.SUCCESS){
            indexingInfo.setEndTime(endTime);

            indexStatusService.update(indexingInfo, status.toString());
            IndexingStep nextStep = indexingInfo.getNextStep().poll();

            logger.info("Indexing {} ==> clusterId: {}, index: {}, collectionId", status, clusterId, indexingInfo.getIndex(), collectionId);
            indexingJobService.changeRefreshInterval(clusterId, collection, index); // 색인 끝나면 refresh_interval 변경

            if (nextStep != null) {
                indexStatusService.create(indexingInfo, "RUNNING"); // Expose가 남아 있기 때문에 status 추가
                indexingInfo.setCurrentStep(nextStep); // 교체 단계 셋팅 (안하면 Null Pointer Exception)
                manageQueue.put(collectionId, indexingInfo); // 교체 단계가 있으므로 다시 스케줄에 넣는다.
            }else{
                deleteQueue.add(collectionId);
            }

            updateLookupQueue(collectionId, status.toString());
            indexerClient.deleteJob(indexingInfo);
            // refresh interval 을 수행한 뒤 60초 대기 시간 추가
            // 다큐먼트 카운트가 전부 업데이트 되기 전에 cat/indices 조회 시 진행 중인 값으로 가져옴
            TimeUtils.sleep(60000);
        }else if (status == IndexerStatus.ERROR || status == IndexerStatus.STOP){
            logger.info("Indexing {} ==> clusterId: {}, index: {}, collectionId", status, clusterId, indexingInfo.getIndex(), collectionId);
            indexingInfo.setEndTime(endTime);
            indexHistoryService.create(indexingInfo, status.toString()); // 로깅
            indexStatusService.update(indexingInfo, status.toString());

            deleteQueue.add(collectionId);
            updateLookupQueue(collectionId, status.toString());
            indexerClient.deleteJob(indexingInfo);
        } else if(status == IndexerStatus.RUNNING){
            indexingInfo.setStatus(IndexerStatus.RUNNING.toString());
            updateLookupQueue(collectionId, status.toString());
            manageQueue.put(collectionId, indexingInfo);
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
            IndexingInfo indexingInfo = lookupQueue.get(collectionId);
            indexingInfo.setStatus(status);
            lookupQueue.replace(collectionId, indexingInfo);
        }
    }
    private void setScheduleQueueStatus(String collectionId, String status){
        if(manageQueue.get(collectionId) != null){
            IndexingInfo currentStatus = manageQueue.get(collectionId);
            currentStatus.setStatus(status);
            manageQueue.replace(collectionId, currentStatus);
        }
    }

    public List<IndexingInfo> getLookupQueueList(){
        List<IndexingInfo> result = new ArrayList<>();
        for (String key : lookupQueue.keySet()) {
            // key 는 collectionId(UUID) 이다
            IndexingInfo indexingInfo = lookupQueue.get(key);
            result.add(indexingInfo);
        }

        return result;
    }

    public List<IndexingInfo> getManageQueueList(){
        List<IndexingInfo> result = new ArrayList<>();
        for (String key : manageQueue.keySet()) {
            // key 는 collectionId(UUID) 이다
            IndexingInfo indexingInfo = manageQueue.get(key);
            result.add(indexingInfo);
        }

        return result;
    }

    public IndexingInfo getCurrentIndexingStatus(String collectionId){
        if(manageQueue.get(collectionId) != null){
            return manageQueue.get(collectionId);
        }
        if(lookupQueue.get(collectionId) != null){
            return lookupQueue.get(collectionId);
        }
        return IndexingInfo.Empty;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}

