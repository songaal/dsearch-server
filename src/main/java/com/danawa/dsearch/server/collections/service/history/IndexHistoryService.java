package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class IndexHistoryService implements HistoryService {
    /**
     * 색인 시 상태 로깅에 대한 책임을 갖는 클래스 입니다.
     */
    private static final Logger logger = LoggerFactory.getLogger(IndexHistoryService.class);

    private IndexHistoryAdapter indexHistoryAdapter;

    private final ElasticsearchFactory elasticsearchFactory;

    public IndexHistoryService(IndexHistoryAdapter indexHistoryAdapter,
                               ElasticsearchFactory elasticsearchFactory) {
        this.indexHistoryAdapter = indexHistoryAdapter;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
//        indicesService.createSystemIndex(clusterId, indexHistory, indexHistoryJson);
    }

    @Override
    public List<Map<String, Object>> findByCollection(UUID clusterId, HistoryReadRequest historyReadRequest) {
        String indexA = historyReadRequest.getIndexA();
        String indexB = historyReadRequest.getIndexB();
        String jobType = historyReadRequest.getJobType();

        if (indexA == null && indexB == null){
            return indexHistoryAdapter.findByClusterIdAndJobType(clusterId, historyReadRequest);
        } else if(jobType == null){
            return indexHistoryAdapter.findByCollection(clusterId, historyReadRequest);
        }else{
            return indexHistoryAdapter.findByCollectionWithJobType(clusterId, historyReadRequest);
        }
    }


    @Override
    public long getTotalSize(UUID clusterId, HistoryReadRequest historyReadRequest) {
        if(historyReadRequest.getIndexA() != null || historyReadRequest.getIndexB() != null ){
            return indexHistoryAdapter.countByClusterIdAndIndex(clusterId, historyReadRequest);
        }else{
            return indexHistoryAdapter.countByClusterId(clusterId);
        }
    }

    @Override
    public void create(IndexingStatus indexingStatus, String status) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        String jobType = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        long endTime = indexingStatus.getEndTime();
        boolean autoRun = indexingStatus.isAutoRun();

        // catIndex 시 bulk indexing이 끝난 경우에도 ES write queue에 적재만 되어 있는 경우가 있기 때문에 30초 대기
        TimeUtils.sleep(30000);
        Map<String, Object> catIndex = elasticsearchFactory.catIndex(clusterId, index);
        String docSize = (String) catIndex.get("docs.count");
        String store = (String) catIndex.get("store.size");

        // 이력의 갯수를 체크하여 0일때만 이력에 남김.
        IndexHistory indexHistory = makeHistory(clusterId, index, jobType, startTime, endTime, autoRun, status, docSize, store);
        indexHistoryAdapter.create(indexHistory);
    }


    @Override
    public void create(IndexingStatus indexingStatus, String docSize, String status, String store) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        String jobType = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        long endTime = indexingStatus.getEndTime();
        boolean autoRun = indexingStatus.isAutoRun();

        IndexHistory indexHistory = makeHistory(clusterId, index, jobType, startTime, endTime, autoRun, status, docSize, store);
        indexHistoryAdapter.create(indexHistory);
    }

    @Override
    public void delete(UUID clusterId, String collectionName){
        indexHistoryAdapter.deleteAll(clusterId, collectionName);
    }

    private IndexHistory makeHistory(UUID clusterId, String index, String jobType, long startTime, long endTime, boolean autoRun, String status, String docSize, String store){
        IndexHistory indexHistory = new IndexHistory();
        indexHistory.setClusterId(clusterId);
        indexHistory.setIndex(index);
        indexHistory.setJobType(jobType);
        indexHistory.setStartTime(startTime);
        indexHistory.setEndTime(endTime);
        indexHistory.setAutoRun(autoRun);
        indexHistory.setStatus(status);
        indexHistory.setDocSize(docSize);
        indexHistory.setStore(store);
        return indexHistory;
    }
}
