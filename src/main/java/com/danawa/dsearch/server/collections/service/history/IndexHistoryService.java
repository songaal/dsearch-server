package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.utils.TimeUtils;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
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

    private Map<String, Object> catIndex(RestHighLevelClient client, String index) throws IOException {
        Request request = new Request("GET", String.format("/_cat/indices/%s", index));
        request.addParameter("format", "json");
        Response response = client.getLowLevelClient().performRequest(request);
        String responseBodyString = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
        return catIndices == null && catIndices.size() > 0 ? new HashMap<>() : catIndices.get(0);
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
//        indicesService.createSystemIndex(clusterId, indexHistory, indexHistoryJson);
    }

    @Override
    public List<Map<String, Object>> findAllByIndexs(UUID clusterId, HistoryReadRequest historyReadRequest) {
        if(historyReadRequest.getJobType() != null){
            return indexHistoryAdapter.findAllByIndexs(clusterId, historyReadRequest);
        }else{
            return indexHistoryAdapter.findAllByIndexsWithJobType(clusterId, historyReadRequest);
        }
    }


    @Override
    public long countByClusterIdAndIndex(UUID clusterId, HistoryReadRequest historyReadRequest) {
        return indexHistoryAdapter.countByClusterIdAndIndex(clusterId, historyReadRequest);
    }

    @Override
    public void create(IndexingStatus indexingStatus, String status) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        String jobType = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        long endTime = indexingStatus.getEndTime();
        boolean autoRun = indexingStatus.isAutoRun();

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            // catIndex 시 bulk indexing이 끝난 경우에도 ES write queue에 적재만 되어 있는 경우가 있기 때문에 1초 대기
            TimeUtils.sleep(1000);

            Map<String, Object> catIndex = catIndex(client, index);
            String docSize = (String) catIndex.get("docs.count");
            String store = (String) catIndex.get("store.size");

            // 이력의 갯수를 체크하여 0일때만 이력에 남김.
            long count = indexHistoryAdapter.count(clusterId, index, startTime, jobType);
            logger.info("{} {} {} {} {} ", clusterId, index, startTime, jobType, count);
            if (count >= 1) {
                IndexHistory indexHistory = makeHistory(clusterId, index, jobType, startTime, endTime, autoRun, status, docSize, store);
                indexHistoryAdapter.create(indexHistory);
            }
        } catch (IOException e) {
            logger.error("히스토리 적재 실패, {}", e);
        }
    }


    @Override
    public void create(IndexingStatus indexingStatus, String docSize, String status, String store) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        String jobType = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        long endTime = indexingStatus.getEndTime();
        boolean autoRun = indexingStatus.isAutoRun();

        // 이력의 갯수를 체크하여 0일때만 이력에 남김.
        long count = indexHistoryAdapter.count(clusterId, index, startTime, jobType);

        if (count == 0) {
            IndexHistory indexHistory = makeHistory(clusterId, index, jobType, startTime, endTime, autoRun, status, docSize, store);
            indexHistoryAdapter.create(indexHistory);
        }
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
