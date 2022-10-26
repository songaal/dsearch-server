package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.notice.NoticeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class CollectionHistoryAndStatusMonitor {

    private static final Logger logger = LoggerFactory.getLogger(CollectionHistoryAndStatusMonitor.class);
    private IndexStatusService indexStatusService;
    private IndexHistoryService indexHistoryService;

    public CollectionHistoryAndStatusMonitor(IndexStatusService indexStatusService,
                                             IndexHistoryService indexHistoryService) {
        this.indexStatusService = indexStatusService;
        this.indexHistoryService = indexHistoryService;
    }

    public void alertWhenIndexingError(String index, String status, String jobType){
        if ("ERROR".equalsIgnoreCase(status)) {
            if ("FULL_INDEX".equalsIgnoreCase(jobType) || "DYNAMIC_INDEX".equalsIgnoreCase(jobType)) {
                NoticeHandler.send(String.format("%s 인덱스의 색인이 실패하였습니다.", index));
            } else {
                NoticeHandler.send(String.format("%s 인덱스의 %s가 실패하였습니다.", index, jobType));
            }
        }
    }

    public void alertWhenStarted(String collectionName, String errMessage){
        NoticeHandler.send(String.format("%s 컬렉션의 색인이 실패하였습니다.\n%s", collectionName, errMessage));
    }

    public void createIndexHistory(IndexingStatus indexingStatus, String docSize, String status, String store){
        alertWhenIndexingError(indexingStatus.getIndex(), status, indexingStatus.getCurrentStep().name());

        while(true){
            try{
                indexHistoryService.createIndexHistory(indexingStatus, docSize, status, store);
            } catch (IOException e) {
                logger.error("엘라스틱서치({}:{}) 연결 실패... {}", indexingStatus.getHost(), indexingStatus.getPort(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                continue;
            }
            break;
        }

    }

    public void createIndexHistory(IndexingStatus indexingStatus, String status){
        alertWhenIndexingError(indexingStatus.getIndex(), status, indexingStatus.getCurrentStep().name());

        while(true){
            try{
                indexHistoryService.createIndexHistory(indexingStatus, status);
            } catch (IOException e) {
                logger.error("엘라스틱서치({}:{}) 연결 실패... {}", indexingStatus.getHost(), indexingStatus.getPort(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                continue;
            }catch (InterruptedException ex){
                continue;
            }
            break;
        }

    }


//    public void deleteLatestStatus(UUID clusterId, String index, long startTime) {
//        boolean isFailed = true;
//
//        while(isFailed){
//            try {
//                indexStatusService.deleteIndexStatus(clusterId, index, startTime);
//            } catch (InterruptedException|IOException e) {
//                logger.error("delete last index status failed... {}", e);
//            }
//            isFailed = false;
//        }
//    }

    public void createIndexStatus(IndexingStatus indexingStatus, String status) {
        while(true){
            try {
                indexStatusService.createIndexStatus(indexingStatus, status);
            } catch (IOException e) {
                logger.error("add last index status failed... {}", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                continue;
            }
            break;
        }
    }

    public void updateIndexStatus(IndexingStatus indexingStatus, String status) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        long startTime = indexingStatus.getStartTime();

        while (true) {
            try {
                indexStatusService.deleteIndexStatus(clusterId, index, startTime);
            } catch (IOException | InterruptedException e) {
                logger.error("delete last index status failed... {}", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                continue;
            }

            try {
                indexStatusService.createIndexStatus(indexingStatus, status);
            } catch (IOException e) {
                logger.error("add last index status failed... {}", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                }
                continue;
            }

            break;
        }
    }
}
