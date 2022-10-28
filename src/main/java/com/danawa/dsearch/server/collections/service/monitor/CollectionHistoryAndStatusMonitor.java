package com.danawa.dsearch.server.collections.service.monitor;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.notice.NoticeHandler;
import com.danawa.dsearch.server.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class CollectionHistoryAndStatusMonitor {
    /**
     * 색인 대상의 상태 및 로깅, 그리고 알림에 대한 책임을 갖는 클래스 입니다.
     *
     * 아래 세개의 객체애 대해 관리합니다.
     * - IndexStatusService: 색인 상태에 대한 책임을 갖습니다
     * - IndexHistoryService: 색인 시 있을 수 있는 로깅에 대한 책임을 갖습니다
     * - Noticehandler: 알림을 담당합니다.
     */

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
                Time.sleep(1000);
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
                Time.sleep(1000);
                continue;
            }catch (InterruptedException ex){
                continue;
            }
            break;
        }

    }

    public void createIndexStatus(IndexingStatus indexingStatus, String status) {
        while(true){
            try {
                indexStatusService.createIndexStatus(indexingStatus, status);
            } catch (IOException e) {
                logger.error("add last index status failed... {}", e);
                Time.sleep(1000);
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
                Time.sleep(1000);
                continue;
            }

            try {
                indexStatusService.createIndexStatus(indexingStatus, status);
            } catch (IOException e) {
                logger.error("add last index status failed... {}", e);
                Time.sleep(1000);
                continue;
            }

            break;
        }
    }
}
