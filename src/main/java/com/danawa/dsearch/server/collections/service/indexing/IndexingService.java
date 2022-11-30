package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexingStep;
import com.danawa.dsearch.server.collections.entity.IndexingAction;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class IndexingService {
    /**
     * 컬렉션의 색인 서비스를 담당하는 객체 입니다.
     * 색인 서비스에는 아래와 같은 타입이 있습니다.
     * - 전체 (All) : 색인 + 교체를 한꺼번에 담당합니다
     * - 색인 (Indexing) : 파일/DB 등 여러 타입을 ES로 색인합니다
     * - 교체 (Expose) : 해당 컬렉션의 Alias를 바꾸어 줍니다.
     * - 중지 (Stop-Indexing) : 색인 중 중지를 하고 싶을때 사용합니다.
     * - 다중 색인(Sub-Start) : 여러 색인 파일을 동시에 하고 싶을때 사용합니다.
     */
    private static Object obj = new Object();

    private static final Logger logger = LoggerFactory.getLogger(IndexingService.class);

    private final IndexingJobManager indexingJobManager;
    private final IndexingJobService indexingJobService;

    public IndexingService(IndexingJobService indexingJobService,
                           IndexingJobManager indexingJobManager){
        this.indexingJobService = indexingJobService;
        this.indexingJobManager = indexingJobManager;
    }
    public Map<String, Object> processIndexingJob(
            UUID clusterId,
            Collection collection,
            IndexingAction actionType,
            String groupSeq) {

        Map<String, Object> response = new HashMap<>();

        switch (actionType){
            case ALL:
                synchronized (obj) {
                    try{
                        IndexingInfo status = startIndexingAndExpose(clusterId, collection);
                        response.put("indexingStatus", status);
                        response.put("result", "success");
                    } catch (IndexingJobFailureException e) {
                        indexingJobManager.setQueueStatus(collection.getId(), "ERROR");
                        response.put("result", "fail");
                    }
                }
                break;
            case INDEXING:
                synchronized (obj) {
                    try {
                        IndexingInfo status = startIndexing(clusterId, collection);
                        response.put("indexingStatus", status);
                        response.put("result", "success");
                    } catch (IndexingJobFailureException e) {
                        indexingJobManager.setQueueStatus(collection.getId(), "ERROR");
                        response.put("result", "fail");
                    }
                }
                break;
            case EXPOSE:
                synchronized (obj) {
                    try{
                        startExpose(clusterId, collection);
                        response.put("result", "success");
                    } catch (IOException | IndexingJobFailureException e) {
                        indexingJobManager.setQueueStatus(collection.getId(), "ERROR");
                        response.put("result", "fail");
                    }
                }
                break;
            case STOP_INDEXING:
                synchronized (obj) {
                    IndexingInfo indexingInfo = stopIndexing(collection);
                    if(indexingInfo == null){
                        response.put("message", "try to stop indexing but not found indexing...");
                    }else{
                        response.put("message", "stopped");
                        response.put("indexingStatus", indexingInfo);
                    }

                    response.put("result", "success");
                }
                break;
            case SUB_START:
                synchronized (obj) {
                    IndexingInfo status = startGroupIndexing(collection, groupSeq);
                    logger.info("GroupIndexing 확인,  {} {}", collection, groupSeq);
                    if (status == null) {
                        response.put("result", "fail");
                    } else {
                        response.put("indexingStatus", status);
                        response.put("result", "success");
                    }
                }
                break;
            default:
                response.put("message", "Not Found Action. Please Select Action in this list (all / indexing / propagate / expose / stop_indexing / stop_propagation)");
                response.put("result", "success");
        }
        return response;
    }

    private boolean isNowIndexing(String collectionId){
        IndexingInfo status = indexingJobManager.getManageQueue(collectionId);
        if (status == null) return false;
        else return true;
    }

    private IndexingInfo startIndexingAndExpose(UUID clusterId, Collection collection) throws IndexingJobFailureException {
        String collectionId = collection.getId();
        if(isNowIndexing(collectionId)){
            throw new IndexingJobFailureException("현재 색인 중...");
        }

        IndexingInfo status = indexingJobService.indexingAndExpose(clusterId, collection);
        indexingJobManager.add(collectionId, status);
        return status;
    }

    private IndexingInfo startIndexing(UUID clusterId, Collection collection) throws IndexingJobFailureException {
        String collectionId = collection.getId();
        if(isNowIndexing(collectionId)){
            throw new IndexingJobFailureException("현재 색인 중...");
        }

        IndexingInfo status = indexingJobService.indexing(clusterId, collection);
        indexingJobManager.add(collectionId, status);
        logger.info("indexingJobManager: {}", indexingJobManager.getManageQueue(collectionId));
        return status;
    }

    private void startExpose(UUID clusterId, Collection collection) throws IndexingJobFailureException, IOException {
        String collectionId = collection.getId();
        if(isNowIndexing(collectionId)){
            throw new IndexingJobFailureException("현재 색인 중...");
        }

        indexingJobService.expose(clusterId, collection);
    }

    private IndexingInfo stopIndexing(Collection collection){
        String collectionId = collection.getId();
        IndexingInfo status = indexingJobManager.getManageQueue(collectionId);

        if (isRightStatusForStopIndexing(status)) {
            indexingJobService.stopIndexing(status);
            indexingJobManager.setQueueStatus(collectionId, "STOP");
        }

        return indexingJobManager.getManageQueue(collectionId);
    }
    private boolean isRightStatusForStopIndexing(IndexingInfo status){
        return status != null
                && (status.getCurrentStep() == IndexingStep.FULL_INDEX
                || status.getCurrentStep() == IndexingStep.DYNAMIC_INDEX );
    }

    private IndexingInfo startGroupIndexing(Collection collection, String groupSeq){
        String collectionId = collection.getId();
        IndexingInfo status = indexingJobManager.getManageQueue(collectionId);

        if (isRightStatusForGroupIndexing(status, groupSeq)) {
            logger.info("sub_start >>>> {}, groupSeq: {}", collection.getName(), groupSeq);
            indexingJobService.startGroupJob(status, collection, groupSeq);
            return status;
        }else{
            return null;
        }
    }
    private boolean isRightStatusForGroupIndexing(IndexingInfo status, String groupSeq){
        return status != null
                && (status.getCurrentStep() == IndexingStep.FULL_INDEX || status.getCurrentStep() == IndexingStep.DYNAMIC_INDEX)
                && isValidGroupSeq(groupSeq);
    }

    private boolean isValidGroupSeq(String groupSeq){
        if (groupSeq == null || "".equalsIgnoreCase(groupSeq)){
            return false;
        }

        try{
            Integer.parseInt(groupSeq);
        }catch (Exception e){
            return false;
        }
        return true;
    }

}
