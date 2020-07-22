package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.entity.IndexStep;
import com.danawa.fastcatx.server.entity.IndexingStatus;
import com.danawa.fastcatx.server.excpetions.IndexingJobFailureException;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexingJobManager {
    private static Logger logger = LoggerFactory.getLogger(IndexingJobManager.class);
    private static RestTemplate restTemplate;

    private final IndexingJobService indexingJobService;
    private final ElasticsearchFactory elasticsearchFactory;

    private final String lastIndexStatusIndex = ".fastcatx_last_index_status";
    private final String indexHistory = ".fastcatx_index_history";

    // KEY: collection id, value: Indexing status
    private ConcurrentHashMap<String, IndexingStatus> jobs = new ConcurrentHashMap<>();

    public IndexingJobManager(IndexingJobService indexingJobService, ElasticsearchFactory elasticsearchFactory) {
        this.indexingJobService = indexingJobService;
        this.elasticsearchFactory = elasticsearchFactory;
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    @Scheduled(cron = "*/5 * * * * *")
    private void fetchIndexingStatus() {
        if (jobs.size() == 0) {
            return;
        }
        Iterator<Map.Entry<String, IndexingStatus>> entryIterator = jobs.entrySet().iterator();
        entryIterator.forEachRemaining(entry -> {
            // key == collectionId
            String id = entry.getKey();
            IndexingStatus indexingStatus = entry.getValue();
            IndexStep step = indexingStatus.getCurrentStep();
            try {
                if (step == null) {
                    throw new Exception("Invalid Step");
                } else if (step == IndexStep.FULL_INDEX || step == IndexStep.DYNAMIC_INDEX) {
                    // indexer한테 상태를 조회한다.
                    updateIndexerStatus(id, indexingStatus);
                } else if (step == IndexStep.PROPAGATE) {
                    // elsticsearch한테 상태를 조회한다.
                    updateElasticsearchStatus(id, indexingStatus);
                } else {
                    logger.error("index: {}, NOT Matched Step.. {}", indexingStatus.getIndex(), step);
                }
            } catch (Exception e) {
                if (indexingStatus.getRetry() > 0) {
                    indexingStatus.setRetry(indexingStatus.getRetry() - 1);
                } else {
                    UUID clusterId = indexingStatus.getClusterId();
                    String index = indexingStatus.getIndex();
                    String jobType = step.name();
                    long startTime = indexingStatus.getStartTime();
                    long endTime = System.currentTimeMillis();
                    boolean autoRun = indexingStatus.isAutoRun();
                    if (step == IndexStep.FULL_INDEX || step == IndexStep.DYNAMIC_INDEX) {
                        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                            jobs.remove(id);
                            deleteLastIndexStatus(client, index, startTime);
                            addIndexHistory(client, index, jobType, startTime, endTime, autoRun, "0", "ERROR", "0");
                            URI deleteUrl = URI.create(String.format("http://%s:%d/async/%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
                            restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), String.class);
                            logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
                        } catch (Exception e1) {
                            logger.error("", e1);
                        }
                    } else if (step == IndexStep.PROPAGATE) {
                        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                            jobs.remove(id);
                            addIndexHistory(client, index, jobType, startTime, endTime, autoRun, "0", "ERROR", "0");
                            logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
                        } catch (Exception e1) {
                            logger.error("", e1);
                        }
                    } else {
                        jobs.remove(id);
                        logger.error("[remove job] retry.. {}", indexingStatus.getRetry());
                    }
                }
            }
        });
    }

    public void add(String collectionId, IndexingStatus indexingStatus) throws IndexingJobFailureException {
        IndexingStatus registerIndexingStatus = findById(collectionId);
        if (indexingStatus.getClusterId() == null) {
            throw new IndexingJobFailureException("Cluster Id Required Field.");
        }
        if (registerIndexingStatus != null) {
            throw new IndexingJobFailureException("Duplicate Collection Indexing");
        }
        if (indexingStatus.getCurrentStep() == null) {
            throw new IndexingJobFailureException("Empty Current Step");
        }

        if (IndexStep.FULL_INDEX == indexingStatus.getCurrentStep() || indexingStatus.getCurrentStep() == IndexStep.DYNAMIC_INDEX) {
            try (RestHighLevelClient client = elasticsearchFactory.getClient(indexingStatus.getClusterId())) {
                addLastIndexStatus(client, indexingStatus.getIndex(), indexingStatus.getStartTime(), "READY");
                jobs.put(collectionId, indexingStatus);
            } catch (IOException e) {
                logger.error("", e);
                throw new IndexingJobFailureException(e);
            }
        }
    }

    public synchronized IndexingStatus findById(String collectionId) {
        return jobs.get(collectionId);
    }

    /**
     * indexer 조회 후 상태 업데이트.
     * */
    private void updateIndexerStatus(String id, IndexingStatus indexingStatus) throws IOException, IndexingJobFailureException {
        URI url = URI.create(String.format("http://%s:%d/async/status?id=%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
        String status = (String) responseEntity.getBody().get("status");
        if ("SUCCESS".equalsIgnoreCase(status) || "ERROR".equalsIgnoreCase(status)) {
            // indexer job id 삭제.
            URI deleteUrl = URI.create(String.format("http://%s:%d/async/%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
            restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), String.class);

            UUID clusterId = indexingStatus.getClusterId();
            String index = indexingStatus.getIndex();
            IndexStep step = indexingStatus.getCurrentStep();
            long startTime = indexingStatus.getStartTime();
            long endTime = System.currentTimeMillis();
            boolean autoRun = indexingStatus.isAutoRun();
            try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                Request request = new Request("GET", String.format("/_cat/indices/%s", index));
                request.addParameter("format", "json");
                request.addParameter("h", "store.size,docs.count");
                Response response = client.getLowLevelClient().performRequest(request);
                String responseBodyString = EntityUtils.toString(response.getEntity());
                List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
                Map<String, Object> catIndex = catIndices.get(0);
                String docSize = (String) catIndex.get("docs.count");
                String store = (String) catIndex.get("store.size");
                deleteLastIndexStatus(client, index, startTime);
                addIndexHistory(client, index, step.name(), startTime, endTime, autoRun, docSize, status.toUpperCase(), store);
            }

            IndexStep nextStep = indexingStatus.getNextStep().poll();
            if ("SUCCESS".equalsIgnoreCase(status) && nextStep != null) {
                indexingStatus.setCurrentStep(nextStep);
                indexingStatus.setRetry(50);
                indexingStatus.setStartTime(System.currentTimeMillis());
                indexingStatus.setEndTime(0L);
                indexingStatus.setAutoRun(true);

                setIndexAlias(clusterId, indexingStatus.getCollection());
                indexingJobService.propagate(clusterId, indexingStatus.getCollection());
                jobs.put(id, indexingStatus);
                logger.debug("next Step >> {}", nextStep);
            } else {
                // 다음 작업이 없으면 제거.
                jobs.remove(id);
                logger.debug("empty next status : {} Step >> {}", status, id);
            }
        } else {
            indexingStatus.setStatus(status);
            jobs.put(id, indexingStatus);
        }
        logger.debug("index: {}, status: {}", indexingStatus.getIndex(), status);
    }


    private void setIndexAlias(UUID clusterId, Collection collection) throws IOException{
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            RestClient restClient = client.getLowLevelClient();
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

            Collection.Index index = indexingJobService.getTargetIndex(collection.getBaseId(), indexA, indexB);
            Collection.Index exposeIndex = null;
            Collection.Index currentIndex = null;

            if(indexA == index){
                currentIndex = indexB;
                exposeIndex = indexA;
            }else{
                currentIndex = indexA;
                exposeIndex = indexB;
            }

            String setJson = "{ \n" +
                    "\"actions\" : [" +
                    "{\n" +
                    "\"add\": {" +
                    "\"index\": \"" + exposeIndex.getIndex() + "\", \n " +
                    "\"alias\": \"" + index.getIndex() + "\" \n" +
                    "}\n" +
                    "},\n" +
                    "{\n" +
                    "\"remove\" : {\n" +
                    "\"index\" : \"" + currentIndex.getIndex() + "\", \n" +
                    "\"alias\" : \"" + index.getIndex() + "\"" +
                    "}\n" +
                    "}\n" +
                    "]\n" + "}";

            Request request = new Request("POST", "_aliases");
            request.setJsonEntity(setJson);
            restClient.performRequest(request);
        }
    }

    /**
     * elasticsearch 조회 후 상태 업데이트.
     * */
    private void updateElasticsearchStatus(String id, IndexingStatus indexingStatus) throws IOException {
//        TODO elasticsearch 전파 상태 조회
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        IndexStep step = indexingStatus.getCurrentStep();
        long startTime = indexingStatus.getStartTime();
        long endTime = System.currentTimeMillis();
        boolean autoRun = indexingStatus.isAutoRun();
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", String.format("/_cat/indices/%s", index));
            request.addParameter("format", "json");
            request.addParameter("h", "store.size,docs.count");
            Response response = client.getLowLevelClient().performRequest(request);
            String responseBodyString = EntityUtils.toString(response.getEntity());
            List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
            Map<String, Object> catIndex = catIndices.get(0);
            String docSize = (String) catIndex.get("docs.count");
            String store = (String) catIndex.get("store.size");
            addIndexHistory(client, index, step.name(), startTime, endTime, autoRun, docSize, "SUCCESS", store);
        } finally {
            logger.info("전파 Success");
            jobs.remove(id);
        }
    }

    public void addIndexHistory(RestHighLevelClient client, String index, String jobType, long startTime, long endTime, boolean autoRun, String docSize, String status, String store) {
        try {
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
        } catch(Exception e) {
            logger.error("addIndexHistory >> index: {}", index, e);
        }
    }

    public void addLastIndexStatus(RestHighLevelClient client, String index, long startTime, String status) {
        try {
            Map<String, Object> source = new HashMap<>();
            source.put("index", index);
            source.put("startTime", startTime);
            source.put("status", status);
            client.index(new IndexRequest().index(lastIndexStatusIndex).source(source), RequestOptions.DEFAULT);
        } catch(Exception e) {
            logger.error("addLastIndexStatus >> index: {}", index, e);
        }
    }
    public void deleteLastIndexStatus(RestHighLevelClient client, String index, long startTime) {
        try {
            client.deleteByQuery(new DeleteByQueryRequest(lastIndexStatusIndex)
                            .setQuery(new BoolQueryBuilder()
                                    .must(new MatchQueryBuilder("index", index))
                                    .must(new MatchQueryBuilder("startTime", startTime)))
                    , RequestOptions.DEFAULT);
        } catch(Exception e) {
            logger.error("deleteLastIndexStatus >> index: {}", index, e);
        }
    }


}
