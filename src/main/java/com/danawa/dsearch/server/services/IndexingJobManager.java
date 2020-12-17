package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.entity.IndexStep;
import com.danawa.dsearch.server.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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

    private final String lastIndexStatusIndex = ".dsearch_last_index_status";
    private final String indexHistory = ".dsearch_index_history";

    // KEY: collection id, value: Indexing status
    private ConcurrentHashMap<String, IndexingStatus> jobs = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, IndexingStatus> indexingProcessQueue = new ConcurrentHashMap<>();
//    private long timeout = 8 * 60 * 60 * 1000;
    @Value("${dsearch.timeout}")
    private long timeout;

    public IndexingJobManager(IndexingJobService indexingJobService, ElasticsearchFactory elasticsearchFactory) {
        this.indexingJobService = indexingJobService;
        this.elasticsearchFactory = elasticsearchFactory;
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    @Scheduled(fixedDelay = 500)
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
                    jobs.remove(id);
                    logger.error("index: {}, NOT Matched Step..", indexingStatus.getIndex());
                    throw new IndexingJobFailureException("STEP is Null");
                } else if (step == IndexStep.FULL_INDEX || step == IndexStep.DYNAMIC_INDEX) {
                    // indexer한테 상태를 조회한다.
                    updateIndexerStatus(id, indexingStatus);
                } else if (step == IndexStep.PROPAGATE) {
                    // elsticsearch한테 상태를 조회한다.
                    updateElasticsearchStatus(id, indexingStatus);
                } else if (step == IndexStep.EXPOSE) {
//                    EXPOSE
                    UUID clusterId = indexingStatus.getClusterId();
                    Collection collection = indexingStatus.getCollection();
                    indexingJobService.expose(clusterId, collection, indexingStatus.getIndex());
                    IndexingStatus idxStat = jobs.get(id);
                    idxStat.setStatus("SUCCESS");
                    idxStat.setEndTime(System.currentTimeMillis());
                    indexingProcessQueue.put(id, idxStat);
                    jobs.remove(id);
                    logger.debug("expose success. collection: {}", collection.getId());
                }

                if (System.currentTimeMillis() - indexingStatus.getStartTime() >= timeout){
                    IndexingStatus status = jobs.get(id);
                    status.setStatus("STOP");
                    status.setEndTime(System.currentTimeMillis());
                    jobs.remove(id);
                    logger.error("index: {}, Timeout 8 hours..", status.getIndex());
                    UUID clusterId = status.getClusterId();
                    try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                        indexingJobService.stopIndexing(indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId());
                        deleteLastIndexStatus(client, status.getIndex(), status.getStartTime());
                        addIndexHistory(client, status.getIndex(),  step.name(), status.getStartTime(), status.getEndTime(), status.isAutoRun(), "0", "ERROR", "0");
                    }catch (Exception e1){
                        logger.error("", e1);
                    }
                }
            } catch (Exception e) {
                logger.error("", e);
                if (indexingStatus.getRetry() > 0) {
                    indexingStatus.setRetry(indexingStatus.getRetry() - 1);
                } else {
                    UUID clusterId = indexingStatus.getClusterId();
                    String index = indexingStatus.getIndex();
                    String jobType = step.name();
                    long startTime = indexingStatus.getStartTime();
                    long endTime = System.currentTimeMillis();
                    boolean autoRun = indexingStatus.isAutoRun();

                    if("STOP".equalsIgnoreCase(indexingStatus.getStatus())){
                        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                            jobs.remove(id);
                            indexingJobService.stopIndexing(indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId());
                            deleteLastIndexStatus(client, index, startTime);
                            addIndexHistory(client, index, jobType, startTime, endTime, autoRun, "0", "ERROR", "0");
                        }catch (Exception e1){
                            logger.error("", e1);
                        }
                    }else if (step == IndexStep.FULL_INDEX || step == IndexStep.DYNAMIC_INDEX) {
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
                            deleteLastIndexStatus(client, index, startTime);
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
        add(collectionId, indexingStatus, true);
    }
    public void add(String collectionId, IndexingStatus indexingStatus, boolean register) throws IndexingJobFailureException {
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

        try (RestHighLevelClient client = elasticsearchFactory.getClient(indexingStatus.getClusterId())) {
            if (register) {
                addLastIndexStatus(client, collectionId, indexingStatus.getIndex(), indexingStatus.getStartTime(), "READY", indexingStatus.getCurrentStep().name(), indexingStatus.getIndexingJobId());
            }
            jobs.put(collectionId, indexingStatus);
        } catch (IOException e) {
            logger.error("", e);
            throw new IndexingJobFailureException(e);
        }
    }

    public IndexingStatus remove(String collectionId) {
        return jobs.remove(collectionId);
    }

    public IndexingStatus findById(String collectionId) {
        return jobs.get(collectionId);
    }


    /**
     * indexer 조회 후 상태 업데이트.
     * */
    private void updateIndexerStatus(String id, IndexingStatus indexingStatus) throws IOException, IndexingJobFailureException {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();

        // check
        URI url = URI.create(String.format("http://%s:%d/async/status?id=%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
        Map<String, Object> body = responseEntity.getBody();
        if (body == null) {
            jobs.remove(id);
            return;
        }
        String status = (String) body.get("status");
        logger.debug("index: {}, status: {}", indexingStatus.getIndex(), status);

        if ("SUCCESS".equalsIgnoreCase(status) || "ERROR".equalsIgnoreCase(status) || "STOP".equalsIgnoreCase(status)) {
            // indexer job id 삭제.
            URI deleteUrl = URI.create(String.format("http://%s:%d/async/%s", indexingStatus.getHost(), indexingStatus.getPort(), indexingStatus.getIndexingJobId()));
            restTemplate.exchange(deleteUrl, HttpMethod.DELETE, new HttpEntity<>(new HashMap<>()), String.class);

            try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                client.getLowLevelClient().performRequest(new Request("POST", String.format("/%s/_flush", index)));
                Map<String, Object> catIndex = catIndex(client, index);
                String docSize = (String) catIndex.get("docs.count");
                String store = (String) catIndex.get("store.size");
                IndexStep step = indexingStatus.getCurrentStep();
                long startTime = indexingStatus.getStartTime();
                long endTime = System.currentTimeMillis();
                deleteLastIndexStatus(client, index, startTime);
                addIndexHistory(client, index, step.name(), startTime, endTime, indexingStatus.isAutoRun(), docSize, status.toUpperCase(), store);
            }

            IndexStep nextStep = indexingStatus.getNextStep().poll();
            if ("SUCCESS".equalsIgnoreCase(status) && nextStep != null) {
                // 다음 작업이 있을 경우.
                indexingStatus = indexingJobService.propagate(clusterId, true, indexingStatus.getCollection(), indexingStatus.getNextStep(), index);
                addLastIndexStatus(clusterId, indexingStatus.getCollection().getId(), index, indexingStatus.getStartTime(), "RUNNING", indexingStatus.getCurrentStep().name());
                jobs.put(id, indexingStatus);

//                IndexingStatus idxStat = jobs.get(id);
//                idxStat.setStatus(status);
//                idxStat.setEndTime(System.currentTimeMillis());
//                indexingProcessQueue.put(id, idxStat);
                logger.debug("next Step >> {}", nextStep);
            } else if ("ERROR".equalsIgnoreCase(status) || "STOP".equalsIgnoreCase(status)) {
                indexingJobService.expose(clusterId, indexingStatus.getCollection());
            } else {
                // 다음 작업이 없으면 제거.
                IndexingStatus idxStat = jobs.get(id);
                idxStat.setStatus("SUCCESS");
                idxStat.setEndTime(System.currentTimeMillis());
                indexingProcessQueue.put(id, idxStat);
                jobs.remove(id);
                logger.debug("empty next status : {} Step >> {}", status, id);
            }
        } else {
            indexingStatus.setStatus(status);
            jobs.put(id, indexingStatus);
        }
    }

    /**
     * elasticsearch 조회 후 상태 업데이트.
     * */
    private void updateElasticsearchStatus(String id, IndexingStatus indexingStatus) throws IOException {
        UUID clusterId = indexingStatus.getClusterId();
        String index = indexingStatus.getIndex();
        IndexStep step = indexingStatus.getCurrentStep();
        boolean done = true;

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", String.format("/%s/_recovery", index));
            request.addParameter("format", "json");
            request.addParameter("filter_path", "**.shards.stage");
            Response response = client.getLowLevelClient().performRequest(request);
            String entityString = EntityUtils.toString(response.getEntity());
            Map<String ,Object> entityMap = new Gson().fromJson(entityString, Map.class);
            List<Map<String, Object>> shards = (List<Map<String, Object>>) ((Map) entityMap.get(index)).get("shards");
            for (int i = 0; i < shards.size(); i++) {
                Map<String, Object> shard = shards.get(i);
                String stage = String.valueOf(shard.get("stage"));
                if (!"DONE".equalsIgnoreCase(stage)) {
                    done = false;
                    break;
                }
            }
        }

        logger.debug("Propagate Check.. index: {}, is success: {}", index, done);
        if (done) {
            try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
                Map<String, Object> catIndex = catIndex(client, index);
                String docSize = (String) catIndex.get("docs.count");
                String store = (String) catIndex.get("store.size");
                long startTime = indexingStatus.getStartTime();
                deleteLastIndexStatus(client, index, startTime);
                addIndexHistory(client, index, step.name(), startTime, System.currentTimeMillis(), indexingStatus.isAutoRun(), docSize, "SUCCESS", store);

                logger.info("PROPAGATE Success");

                IndexStep nextStep = indexingStatus.getNextStep().poll();
                if (nextStep != null) {
                    indexingStatus.setCurrentStep(nextStep);
                    indexingStatus.setStartTime(System.currentTimeMillis());
                    indexingStatus.setEndTime(0);
                    indexingStatus.setRetry(50);
                    indexingStatus.setAutoRun(true);
                    jobs.put(id, indexingStatus);
//                    IndexingStatus idxStat = jobs.get(id);
//                    idxStat.setStatus("SUCCESS");
//                    idxStat.setEndTime(System.currentTimeMillis());
//                    indexingProcessQueue.put(id, idxStat);
                    logger.debug("add next job : {} ", nextStep.name());
                } else {
                    IndexingStatus idxStat = jobs.get(id);
                    idxStat.setStatus("SUCCESS");
                    idxStat.setEndTime(System.currentTimeMillis());
                    indexingProcessQueue.put(id, idxStat);
                    jobs.remove(id);
                    logger.debug("empty next status : {} ", id);
                }
            }
        }
    }

    private Map<String ,Object> catIndex(RestHighLevelClient client, String index) throws IOException {
        Request request = new Request("GET", String.format("/_cat/indices/%s", index));
        request.addParameter("format", "json");
        Response response = client.getLowLevelClient().performRequest(request);
        String responseBodyString = EntityUtils.toString(response.getEntity());
        List<Map<String, Object>> catIndices = new Gson().fromJson(responseBodyString, List.class);
        return catIndices == null && catIndices.size() > 0 ? new HashMap<>() : catIndices.get(0);
    }

    public void addIndexHistory(RestHighLevelClient client, String index, String jobType, long startTime, long endTime, boolean autoRun, String docSize, String status, String store) {

        if ("ERROR".equalsIgnoreCase(status)) {
            if ("FULL_INDEX".equalsIgnoreCase(jobType) || "DYNAMIC_INDEX".equalsIgnoreCase(jobType)) {
                NoticeHandler.send("%s 인덱스의 색인이 실패하였습니다.");
            } else if ("PROPAGATE".equalsIgnoreCase(jobType)) {
                NoticeHandler.send("%s 인덱스의 전파가 실패하였습니다.");
            }
        }

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

    public void addLastIndexStatus(UUID clusterId, String collectionId, String index, long startTime, String status, String step) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            addLastIndexStatus(client, collectionId, index, startTime, status, step, null);
        }
    }
    public void addLastIndexStatus(RestHighLevelClient client, String collectionId, String index, long startTime, String status, String step, String jobId) {
        try {
            Map<String, Object> source = new HashMap<>();
            source.put("collectionId", collectionId);
            source.put("index", index);
            source.put("startTime", startTime);
            source.put("status", status);
            source.put("step", step);
            source.put("jobId", jobId);
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

    public Map<String, Object> getIndexingStatus(){
        Map<String, Object> map = new HashMap<>();
        for(String key : jobs.keySet()){
            Map<String, Object> template = new HashMap<>();
            template.put("server", jobs.get(key));

            map.put(key, template);
        }
        return map;
    }

    public void setStopStatus(String collectionId, String status){
        if(jobs.get(collectionId) != null){
            IndexingStatus currentStatus = jobs.get(collectionId);
            currentStatus.setStatus("STOP");
            indexingProcessQueue.put(collectionId, currentStatus);
        }

        if(indexingProcessQueue.get(collectionId) != null){
            IndexingStatus indexingStatus = indexingProcessQueue.get(collectionId);
            indexingStatus.setStatus("STOP");
            indexingProcessQueue.replace(collectionId, indexingStatus);
        }
    }

    public IndexingStatus getIndexingStatus(String collectionId){

        if(jobs.get(collectionId) != null){
            IndexingStatus currentStatus = jobs.get(collectionId);
            IndexingStatus idxStatus = new IndexingStatus();
            idxStatus.setAction(currentStatus.getAction());
            idxStatus.setClusterId(currentStatus.getClusterId());
            idxStatus.setCurrentStep(currentStatus.getCurrentStep());
            idxStatus.setCollection(currentStatus.getCollection());
            idxStatus.setEndTime(currentStatus.getEndTime());
            idxStatus.setError(currentStatus.getError());
            idxStatus.setHost(currentStatus.getHost());
            idxStatus.setIndexingJobId(currentStatus.getIndexingJobId());
            idxStatus.setNextStep(currentStatus.getNextStep());
            idxStatus.setPort(currentStatus.getPort());
            idxStatus.setStartTime(currentStatus.getStartTime());
            idxStatus.setStatus("RUNNING");
            return idxStatus;
        }

        if(indexingProcessQueue.get(collectionId) != null){
            IndexingStatus indexingStatus = indexingProcessQueue.get(collectionId);
            return indexingStatus;
        }

        return null;
    }

    public Map<String, Object> getSettings(){
        Map<String, Object> settings = new HashMap<>();
        settings.put("indexing", this.indexingJobService.getIndexingSettings());
        settings.put("propagate", this.indexingJobService.getPropagateSettings());
        return settings;
    }
    public void setSettings(String type, Map<String, Object> settings){
        if(type.equals("indexing")){
            this.indexingJobService.setIndexingSettings(settings);
        }else if(type.equals("propagate")){
            this.indexingJobService.setPropagateSettings(settings);
        }
    }

    public void setTimeout(long timeout){
        this.timeout = timeout;
    }
}
