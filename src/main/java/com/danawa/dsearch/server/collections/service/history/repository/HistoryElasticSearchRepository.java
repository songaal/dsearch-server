package com.danawa.dsearch.server.collections.service.history.repository;

import com.danawa.dsearch.server.collections.entity.IndexHistory;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.utils.TimeUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class HistoryElasticSearchRepository {
    private final String indexHistory = ".dsearch_index_history";
    private final String indexHistoryJson = "index_history.json";
    private final ElasticsearchFactory elasticsearchFactory;

    public HistoryElasticSearchRepository(ElasticsearchFactory elasticsearchFactory){
        this.elasticsearchFactory =elasticsearchFactory;
    }

    public void save(UUID clusterId, IndexHistory indexHistory) {
        while(true){
            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                // 이력의 갯수를 체크하여 0일때만 이력에 남김.
                BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
                countQuery.filter().add(QueryBuilders.termQuery("index", indexHistory.getIndex()));
                countQuery.filter().add(QueryBuilders.termQuery("startTime", indexHistory.getStartTime()));
                countQuery.filter().add(QueryBuilders.termQuery("jobType", indexHistory.getJobType()));

                CountResponse countResponse = client.count(new CountRequest(this.indexHistory).query(countQuery), RequestOptions.DEFAULT);
                if (countResponse.getCount() == 0) {
                    Map<String, Object> source = new HashMap<>();
                    source.put("index", indexHistory.getIndex());
                    source.put("jobType", indexHistory.getJobType());
                    source.put("startTime", indexHistory.getStartTime());
                    source.put("endTime", indexHistory.getEndTime());
                    source.put("autoRun", indexHistory.isAutoRun());
                    source.put("status", indexHistory.getStatus());
                    source.put("docSize", indexHistory.getDocSize());
                    source.put("store", indexHistory.getStore());
                    client.index(new IndexRequest().index(this.indexHistory).source(source), RequestOptions.DEFAULT);
                }
            }catch (IOException e){
                TimeUtils.sleep(1000);
                continue;
            }
            break;
        }
    }


//    public void create(IndexingStatus indexingStatus, String status){
//        UUID clusterId = indexingStatus.getClusterId();
//        String index = indexingStatus.getIndex();
//        String jobType = indexingStatus.getCurrentStep().name();
//        long startTime = indexingStatus.getStartTime();
//        long endTime = indexingStatus.getEndTime();
//        boolean autoRun = indexingStatus.isAutoRun();
//
//        while(true){
//            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//                // catIndex 시 bulk indexing이 끝난 경우에도 ES write queue에 적재만 되어 있는 경우가 있기 때문에 1초 대기
//                TimeUtils.sleep(1000);
//
//                Map<String, Object> catIndex = catIndex(client, index);
//                String docSize = (String) catIndex.get("docs.count");
//                String store = (String) catIndex.get("store.size");
//
//                // 이력의 갯수를 체크하여 0일때만 이력에 남김.
//                BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
//                countQuery.filter().add(QueryBuilders.termQuery("index", index));
//                countQuery.filter().add(QueryBuilders.termQuery("startTime", startTime));
//                countQuery.filter().add(QueryBuilders.termQuery("jobType", jobType));
//
//                CountResponse countResponse = client.count(new CountRequest(indexHistory).query(countQuery), RequestOptions.DEFAULT);
//                if (countResponse.getCount() == 0) {
//                    Map<String, Object> source = new HashMap<>();
//                    source.put("index", index);
//                    source.put("jobType", jobType);
//                    source.put("startTime", startTime);
//                    source.put("endTime", endTime);
//                    source.put("autoRun", autoRun);
//                    source.put("status", status);
//                    source.put("docSize", docSize);
//                    source.put("store", store);
//                    client.index(new IndexRequest().index(indexHistory).source(source), RequestOptions.DEFAULT);
//                }
//            } catch (IOException e) {
//                TimeUtils.sleep(1000);
//                continue;
//            }
//            break;
//        }
//
//    }
//
//    public void create(IndexingStatus indexingStatus, String docSize, String status, String store) {
//        UUID clusterId = indexingStatus.getClusterId();
//        String index = indexingStatus.getIndex();
//        String jobType = indexingStatus.getCurrentStep().name();
//        long startTime = indexingStatus.getStartTime();
//        long endTime = indexingStatus.getEndTime();
//        boolean autoRun = indexingStatus.isAutoRun();
//
//        while(true){
//            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
//                // 이력의 갯수를 체크하여 0일때만 이력에 남김.
//                BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
//                countQuery.filter().add(QueryBuilders.termQuery("index", index));
//                countQuery.filter().add(QueryBuilders.termQuery("startTime", startTime));
//                countQuery.filter().add(QueryBuilders.termQuery("jobType", jobType));
//
//                CountResponse countResponse = client.count(new CountRequest(indexHistory).query(countQuery), RequestOptions.DEFAULT);
//                logger.debug("addIndexHistory: index: {}, startTime: {}, jobType: {}, result Count: {}", index, startTime, jobType, countResponse.getCount());
//                if (countResponse.getCount() == 0) {
//                    Map<String, Object> source = new HashMap<>();
//                    source.put("index", index);
//                    source.put("jobType", jobType);
//                    source.put("startTime", startTime);
//                    source.put("endTime", endTime);
//                    source.put("autoRun", autoRun);
//                    source.put("status", status);
//                    source.put("docSize", docSize);
//                    source.put("store", store);
//                    client.index(new IndexRequest().index(indexHistory).source(source), RequestOptions.DEFAULT);
//                }
//            } catch (IOException e) {
//                TimeUtils.sleep(1000);
//                continue;
//            }
//            break;
//        }
//
//    }
}
