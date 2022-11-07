package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingActionType;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

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
    public void processIndexingJob(
            UUID clusterId,
            String clientIP,
            String id,
            Collection collection,
            IndexingActionType actionType,
            String groupSeq,
            Map<String, Object> response) {

        logger.info("clusterId={}, clientIP={}, id={}, collection={}, actionType={}, groupSeq={}, response={}",
                clusterId,
                clientIP,
                id,
                collection,
                actionType,
                groupSeq,
                response);

        switch (actionType){
            case ALL:
                synchronized (obj) {
                    try{
                        IndexingStatus status = startIndexingAll(clusterId, collection, actionType);
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
                        IndexingStatus status = startIndexing(clusterId, collection, actionType);
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
                    IndexingStatus indexingStatus = stopIndexing(collection);
                    if(indexingStatus == null){
                        response.put("message", "try to stop indexing but not found indexing...");
                    }else{
                        response.put("message", "stopped");
                        response.put("indexingStatus", indexingStatus);
                    }

                    response.put("result", "success");
                }
                break;
            case SUB_START:
                synchronized (obj) {
                    IndexingStatus status = startIndexingForSubStart(collection, groupSeq);
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
    }

    public IndexingStatus startIndexingAll(UUID clusterId, Collection collection, IndexingActionType actionType) throws IndexingJobFailureException {
        String collectionId = collection.getId();

        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);
        if(status != null){
            throw new IndexingJobFailureException("기존 데이터가 있음");
        }

        Queue<IndexActionStep> nextStep = new ArrayDeque<>();
        nextStep.add(IndexActionStep.EXPOSE);
        status = indexingJobService.indexing(clusterId, collection, true, IndexActionStep.FULL_INDEX, nextStep);
        status.setAction(actionType.getAction());
        status.setStatus("RUNNING");
        indexingJobManager.add(collectionId, status);
        return status;
    }

    public IndexingStatus startIndexing(UUID clusterId, Collection collection, IndexingActionType actionType) throws IndexingJobFailureException {
        String collectionId = collection.getId();

        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);
        if(status != null){
            throw new IndexingJobFailureException("기존 데이터가 있음");
        }

        status = indexingJobService.indexing(clusterId, collection, false, IndexActionStep.FULL_INDEX);
        status.setAction(actionType.getAction());
        status.setStatus("RUNNING");
        indexingJobManager.add(collectionId, status);
        logger.info("indexingJobManager: {}", indexingJobManager.getManageQueue(collectionId));
        return status;
    }

    public void startExpose(UUID clusterId, Collection collection) throws IndexingJobFailureException, IOException {
        String collectionId = collection.getId();

        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);
        if(status != null){
            throw new IndexingJobFailureException("기존 데이터가 있음");
        }
        indexingJobService.expose(clusterId, collection);
    }

    public IndexingStatus stopIndexing(Collection collection){
        String collectionId = collection.getId();
        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);

        if (isRightStatusForStopIndexing(status)) {
            indexingJobService.stopIndexing(status);
            indexingJobManager.setQueueStatus(collectionId, "STOP");
        }

        return indexingJobManager.getManageQueue(collectionId);
    }
    private boolean isRightStatusForStopIndexing(IndexingStatus status){
        return status != null
                && (status.getCurrentStep() == IndexActionStep.FULL_INDEX
                || status.getCurrentStep() == IndexActionStep.DYNAMIC_INDEX );
    }

    public IndexingStatus startIndexingForSubStart(Collection collection, String groupSeq){
        String collectionId = collection.getId();
        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);

        if (isRightStatusForSubStart(status, groupSeq)) {
            logger.info("sub_start >>>> {}, groupSeq: {}", collection.getName(), groupSeq);
            indexingJobService.subStart(status, collection, groupSeq);
            return status;
        }else{
            return null;
        }
    }
    private boolean isRightStatusForSubStart(IndexingStatus status, String groupSeq){
        return status != null && (status.getCurrentStep() == IndexActionStep.FULL_INDEX || status.getCurrentStep() == IndexActionStep.DYNAMIC_INDEX) && groupSeq != null && !"".equalsIgnoreCase(groupSeq);
    }
}
