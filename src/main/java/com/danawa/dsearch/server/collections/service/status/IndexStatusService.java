package com.danawa.dsearch.server.collections.service.status;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.collections.entity.IndexStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class IndexStatusService implements StatusService {
    /**
     * 색인 상태에 대해서 관리하는 클래스 입니다.
     */

    private static final Logger logger = LoggerFactory.getLogger(IndexStatusService.class);

//    private final ElasticsearchFactory elasticsearchFactory;
//    private final String lastIndexStatusIndex = ".dsearch_last_index_status";
//
//    private final String lastIndexStatusIndexJson = "last_index_status.json";

//    private IndicesService indicesService;

    private IndexStatusAdapter indexStatusAdapter;

    public IndexStatusService(
//            ElasticsearchFactory elasticsearchFactory,
//                              IndicesService indicesService,
                              IndexStatusAdapter indexStatusAdapter) {
//        this.elasticsearchFactory = elasticsearchFactory;
//        this.indicesService = indicesService;
        this.indexStatusAdapter = indexStatusAdapter;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
//        indicesService.createSystemIndex(clusterId, lastIndexStatusIndex, lastIndexStatusIndexJson);
    }

    @Override
    public void create(IndexingStatus status, String currentStatus) {
        UUID clusterId = status.getClusterId();
        String collectionId = status.getCollection().getId();
        long startTime = status.getStartTime();
        String index = status.getIndex();
        String step = status.getCurrentStep().name();
        String jobId = status.getIndexingJobId();

        IndexStatus indexStatus = makeStatusEntity(clusterId, collectionId, index, startTime, currentStatus, step, jobId);
        indexStatusAdapter.create(indexStatus);
    }

    private IndexStatus makeStatusEntity(UUID clusterId, String collectionId, String index, long startTime, String status, String step, String jobId){
        IndexStatus indexStatus = new IndexStatus();
        indexStatus.setClusterId(clusterId);
        indexStatus.setCollectionId(collectionId);
        indexStatus.setIndex(index);
        indexStatus.setStartTime(startTime);
        indexStatus.setStatus(status);
        indexStatus.setStep(step);
        indexStatus.setJobId(jobId);
        return indexStatus;
    }

    @Override
    public void delete(UUID clusterId, String index, long startTime) {
        indexStatusAdapter.delete(clusterId, index, startTime);
    }

    @Override
    public void update(IndexingStatus indexingStatus, String status) {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        long startTime = indexingStatus.getStartTime();

        delete(clusterId, index, startTime);
        create(indexingStatus, status);
    }

    @Override
    public List<Map<String, Object>> findAll(UUID clusterId, int size, int from) {
        List<IndexStatus> list = indexStatusAdapter.findAll(clusterId, size, from);
        return convertToList(list);
    }

    private List<Map<String, Object>> convertToList(List<IndexStatus> list){
        List<Map<String, Object>> result = new ArrayList<>();

        for (IndexStatus entity: list){
            Map<String, Object> item = new HashMap<>();
            item.put("jobId", entity.getJobId());
            item.put("index", entity.getIndex());
            item.put("startTime", entity.getStartTime());
            item.put("step", entity.getStep());
            result.add(item);
        }

        return result;
    }


//    @Override
//    public void create(IndexingStatus status, String currentStatus) {
//        UUID clusterId = status.getClusterId();
//        String collectionId = status.getCollection().getId();
//        long startTime = status.getStartTime();
//        String index = status.getIndex();
//        String step = status.getCurrentStep().name();
//        String jobId = status.getIndexingJobId();
//
//        while (true) {
//            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//                createIndexStatus(client, collectionId, index, startTime, currentStatus, step, jobId);
//            } catch (IOException e) {
//                TimeUtils.sleep(1000);
//                continue;
//            }
//            break;
//        }
//
//    }

//    private void createIndexStatus(RestHighLevelClient client, String collectionId, String index, long startTime, String status, String step, String jobId) throws IOException {
//        Map<String, Object> source = new HashMap<>();
//        source.put("collectionId", collectionId);
//        source.put("index", index);
//        source.put("startTime", startTime);
//        source.put("status", status);
//        source.put("step", step);
//        source.put("jobId", jobId);
//        client.index(new IndexRequest().index(lastIndexStatusIndex).source(source), RequestOptions.DEFAULT);
//    }

//    @Override
//    public void delete(UUID clusterId, String index, long startTime) throws IOException, InterruptedException {
//        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//            logger.debug("deleteLastIndexStatus index: {} , startTime: {}", index, startTime);
//            for (int i = 0; i < 3; i++) {
//                BulkByScrollResponse response;
//                if (startTime > 0) {
//                    response = client.deleteByQuery(new DeleteByQueryRequest(lastIndexStatusIndex)
//                                    .setQuery(new BoolQueryBuilder()
//                                            .must(new MatchQueryBuilder("index", index))
//                                            .must(new MatchQueryBuilder("startTime", startTime)))
//                            , RequestOptions.DEFAULT);
//                } else {
//                    response = client.deleteByQuery(new DeleteByQueryRequest(lastIndexStatusIndex)
//                                    .setQuery(new BoolQueryBuilder()
//                                            .must(new MatchQueryBuilder("index", index)))
//                            , RequestOptions.DEFAULT);
//                }
//                if (response != null && response.getDeleted() == 0) {
//                    TimeUtils.sleep(1000);
//                } else {
//                    break;
//                }
//            }
//        }
//    }
//@Override
//public void update(IndexingStatus indexingStatus, String status) {
//    UUID clusterId = indexingStatus.getClusterId();
//    String index = indexingStatus.getIndex();
//    long startTime = indexingStatus.getStartTime();
//
//    while (true) {
//        try {
//            delete(clusterId, index, startTime);
//        } catch (IOException | InterruptedException e) {
//            logger.error("delete last index status failed... {}", e);
//            TimeUtils.sleep(1000);
//            continue;
//        }
//
//        create(indexingStatus, status);
//    }
//}
//@Override
//public SearchResponse findAll(UUID clusterId, int size, int from) throws IOException {
//    try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//        return client.search(new SearchRequest(lastIndexStatusIndex)
//                        .source(new SearchSourceBuilder()
//                                .size(size)
//                                .from(from))
//                , RequestOptions.DEFAULT);
//    }
//}
}
