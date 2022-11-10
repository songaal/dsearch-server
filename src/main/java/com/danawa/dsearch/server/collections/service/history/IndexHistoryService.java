package com.danawa.dsearch.server.collections.service.history;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.utils.TimeUtils;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class IndexHistoryService implements HistoryService{
    /**
     * 색인 시 상태 로깅에 대한 책임을 갖는 클래스 입니다.
     */
    private static final Logger logger = LoggerFactory.getLogger(IndexHistoryService.class);

    private final String indexHistory = ".dsearch_index_history";
    private final String indexHistoryJson = "index_history.json";

    private final ElasticsearchFactory elasticsearchFactory;
    private IndicesService indicesService;

    public IndexHistoryService(IndicesService indicesService,
                               ElasticsearchFactory elasticsearchFactory){
        this.elasticsearchFactory = elasticsearchFactory;
        this.indicesService = indicesService;
    }

    private Map<String ,Object> catIndex(RestHighLevelClient client, String index) throws IOException {
        Request request = new Request("GET", String.format("/_cat/indices/%s", index));
        request.addParameter("format", "json");
        Response response = client.getLowLevelClient().performRequest(request);
        String responseBodyString = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
        return catIndices == null && catIndices.size() > 0 ? new HashMap<>() : catIndices.get(0);
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, indexHistory, indexHistoryJson);
    }

    @Override
    public void create(IndexingStatus indexingStatus, String status){
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        String jobType = indexingStatus.getCurrentStep().name();
        long startTime = indexingStatus.getStartTime();
        long endTime = indexingStatus.getEndTime();
        boolean autoRun = indexingStatus.isAutoRun();

        while(true){
            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                // catIndex 시 bulk indexing이 끝난 경우에도 ES write queue에 적재만 되어 있는 경우가 있기 때문에 30초 대기
                TimeUtils.sleep(30000);

                Map<String, Object> catIndex = catIndex(client, index);
                String docSize = (String) catIndex.get("docs.count");
                String store = (String) catIndex.get("store.size");
                logger.info("index={} docSize={} store={}", index, docSize, store);

                // 이력의 갯수를 체크하여 0일때만 이력에 남김.
                BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
                countQuery.filter().add(QueryBuilders.termQuery("index", index));
                countQuery.filter().add(QueryBuilders.termQuery("startTime", startTime));
                countQuery.filter().add(QueryBuilders.termQuery("jobType", jobType));

                CountResponse countResponse = client.count(new CountRequest(indexHistory).query(countQuery), RequestOptions.DEFAULT);
                logger.debug("addIndexHistory: index: {}, startTime: {}, jobType: {}, result Count: {}", index, startTime, jobType, countResponse.getCount());
                if (countResponse.getCount() == 0) {
                    Map<String, Object> source = new HashMap<>();
                    source.put("index", index);
                    source.put("jobType", jobType);
                    source.put("startTime", startTime);
                    source.put("endTime", endTime);
                    source.put("autoRun", autoRun);
                    source.put("status", status);
                    source.put("docSize", docSize);
                    source.put("store", store);
                    client.index(new IndexRequest().index(indexHistory).source(source), RequestOptions.DEFAULT);
                }
            } catch (IOException e) {
                TimeUtils.sleep(1000);
                continue;
            }
            break;
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

        while(true){
            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                // 이력의 갯수를 체크하여 0일때만 이력에 남김.
                BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
                countQuery.filter().add(QueryBuilders.termQuery("index", index));
                countQuery.filter().add(QueryBuilders.termQuery("startTime", startTime));
                countQuery.filter().add(QueryBuilders.termQuery("jobType", jobType));

                CountResponse countResponse = client.count(new CountRequest(indexHistory).query(countQuery), RequestOptions.DEFAULT);
                logger.debug("addIndexHistory: index: {}, startTime: {}, jobType: {}, result Count: {}", index, startTime, jobType, countResponse.getCount());
                if (countResponse.getCount() == 0) {
                    Map<String, Object> source = new HashMap<>();
                    source.put("index", index);
                    source.put("jobType", jobType);
                    source.put("startTime", startTime);
                    source.put("endTime", endTime);
                    source.put("autoRun", autoRun);
                    source.put("status", status);
                    source.put("docSize", docSize);
                    source.put("store", store);
                    client.index(new IndexRequest().index(indexHistory).source(source), RequestOptions.DEFAULT);
                }
            } catch (IOException e) {
                TimeUtils.sleep(1000);
                continue;
            }
            break;
        }

    }
}
