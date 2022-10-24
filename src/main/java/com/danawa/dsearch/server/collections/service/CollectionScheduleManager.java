package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.excpetions.CronParseException;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Service
public class CollectionScheduleManager {
    private static Logger logger = LoggerFactory.getLogger(CollectionScheduleManager.class);
    private ConcurrentHashMap<String, ScheduledFuture<?>> schedulerMap = new ConcurrentHashMap<>();
    private final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();

    private ClusterService clusterService ;
    private ElasticsearchFactory elasticsearchFactory;
    private final CollectionService collectionService;
    private final IndexingJobService indexingJobService;
    private final IndexingJobManager indexingJobManager;
    private final String lastIndexStatusIndex = ".dsearch_last_index_status";

    public CollectionScheduleManager(ClusterService clusterService,
                                     CollectionService collectionService,
                                     ElasticsearchFactory elasticsearchFactory,
                                     IndexingJobService indexingJobService,
                                     IndexingJobManager indexingJobManager){
        this.indexingJobService = indexingJobService;
        this.indexingJobManager = indexingJobManager;
        this.collectionService = collectionService;
        this.clusterService = clusterService;
        this.elasticsearchFactory = elasticsearchFactory;
        this.scheduler.initialize();
    }

    @PostConstruct
    public void init() {
//        1. 등록된 모든 클러스터 조회
        List<Cluster> clusterList = clusterService.findAll();

        int clusterSize = clusterList.size();

        for (int i = 0; i < clusterSize; i++) {
            Cluster cluster = clusterList.get(i);

            try (RestHighLevelClient client = elasticsearchFactory.getClient(cluster.getId())) {
//                1. 클러스터별로 기존 작업 중인 잡을 다시 등록한다.
                SearchResponse lastIndexResponse = client.search(new SearchRequest(lastIndexStatusIndex)
                                .source(new SearchSourceBuilder()
                                        .size(10000)
                                        .from(0))
                        , RequestOptions.DEFAULT);

                Calendar calendar = Calendar.getInstance();
                // 진행 중 문서 중 jobID가 null 인경우와 7일 지난 문서는 무시.
                calendar.add(Calendar.DATE, 7);
                long expireStartTime = calendar.getTimeInMillis();
                lastIndexResponse.getHits().forEach(documentFields -> {
                    try {
                        Map<String, Object> source = documentFields.getSourceAsMap();
                        // 잡아이디가 없는 문서가 많음... 로컬 실행하여 테스트 데이터 의심...
                        if (source.get("jobId") != null && !"".equals(source.get("jobId"))) {
                            String collectionId = (String) source.get("collectionId");
                            String jobId = String.valueOf(source.getOrDefault("jobId", ""));
                            String index = (String) source.get("index");
                            long startTime = (long) source.get("startTime");
                            IndexActionStep step = IndexActionStep.valueOf(String.valueOf(source.get("step")));

                            if (expireStartTime <= startTime) {
                                Collection collection = collectionService.findById(cluster.getId(), collectionId);
                                Collection.Launcher launcher = collection.getLauncher();
                                IndexingStatus indexingStatus = new IndexingStatus();
                                indexingStatus.setClusterId(cluster.getId());
                                indexingStatus.setIndex(index);
                                indexingStatus.setStartTime(startTime);
                                if (launcher != null) {
                                    indexingStatus.setIndexingJobId(jobId);
                                    indexingStatus.setScheme(launcher.getScheme());
                                    indexingStatus.setHost(launcher.getHost());
                                    indexingStatus.setPort(launcher.getPort());
                                }
                                indexingStatus.setAutoRun(true);
                                indexingStatus.setCurrentStep(step);
                                indexingStatus.setNextStep(new ArrayDeque<>());
                                indexingStatus.setRetry(50);
                                indexingStatus.setCollection(collection);

                                if(indexingJobManager.getScheduleQueue(collectionId) == null) {
                                    indexingJobManager.add(collectionId, indexingStatus, false);
                                    logger.debug("collection register cluster: {}, job: {}, step: {}", cluster.getName(), collectionId, step);
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("[INIT ERROR] ", e);
                    }
                });
            } catch (Exception e) {
                logger.error("[INIT ERROR]", e);
            }

            // 컬렉션의 스케쥴이 enabled 이면 다시 스케쥴을 등록한다.
            registerAllSchedule(cluster.getId());
            logger.debug("init finished");
        }
    }

    /**
     * 키로 등록된 크론잡을 제거한다
     * @param key
     */
    private void removeCronJob(String key){
        ScheduledFuture<?> future = schedulerMap.get(key);
        future.cancel(true);
        schedulerMap.remove(key);
    }

    /**
     * 모든 스케줄 삭제
     * @param clusterId
     */
    public void removeAllSchedule(UUID clusterId){
        if(clusterId == null) throw new NullArgumentException("");

        for(String key : schedulerMap.keySet()){
            if(isRemovableKey(key, clusterId)){
                logger.info("스케줄 제거, clusterId: {}, scheduled key: {}", clusterId, key);
                removeCronJob(key);
            }
        }
    }

    /**
     * 컬렉션에 등록된 많은 크론잡을 스케줄에서 제거한다
     * @param clusterId
     * @param collectionId
     */
    private void removeCollectionSchedule(UUID clusterId, String collectionId){
        Iterator<String> keys = schedulerMap.keySet().iterator();
        while (keys.hasNext()){
            String key = keys.next();
            if(isRemovableKey(key, clusterId, collectionId)){
                logger.info("Schedule Removed key : {}", key);
                removeCronJob(key);
                logger.info("{} is deleted status: {}", key, isAliveSchedule(key));
            }
        }
    }


    /**
     * 키가 지울수 있는 키인지 확인한다
     * @param key
     * @param clusterId
     * @return
     */
    private boolean isRemovableKey(String key, UUID clusterId){
        String prefix = clusterId.toString();
        if(key.startsWith(prefix)){
            return true;
        }
        return false;
    }

    /**
     * 키가 지울수 있는 키인지 확인한다
     * @param key
     * @param clusterId
     * @param collectionId
     * @return
     */
    private boolean isRemovableKey(String key, UUID clusterId, String collectionId){
        String prefix = String.format("%s-%s", clusterId, collectionId);
        if(key.startsWith(prefix)){
            return true;
        }
        return false;
    }

    /**
     * 스케줄을 리셋해서 등록한다
     * @param clusterId
     * @param collectionId
     */
    public void resetCollectionSchedule(UUID clusterId, String collectionId){
        removeCollectionSchedule(clusterId, collectionId); // 기존 스케줄을 전부 지우고
        registerCollectionSchedule(clusterId, collectionId); //새로 스케줄을 등록한다
    }

    /**
     * 클러스터 내에 있는 모든 스케줄이 활성화된 컬렉션들에 대해 스케줄 매니저에 등록한다
     * @param clusterId
     */
    public void registerAllSchedule(UUID clusterId){
        if(clusterId == null) throw new NullArgumentException("");

        try {
            /** 컬렉션의 스케쥴이 enabled 이면 다시 스케쥴을 등록한다. */
            List<Collection> collectionList = collectionService.findAll(clusterId);
            if (collectionList != null) {
                collectionList.forEach(collection -> {
                    try {
                        if(collection.isScheduled()) {
                            registerCronJob(clusterId, collection.getId(), collection.getCron());
                        }
                    } catch (Exception e) {
                        logger.error("[Register schedule ERROR] ", e);
                    }
                });
            }
        } catch (Exception e) {
            logger.error("[Register schedule ERROR]", e);
        }
    }

    /**
     * 클러스터 내에 있는 컬렉션 한개에 대해 스케줄을 등록한다
     * @param clusterId
     * @param collectionId
     */
    public void registerCollectionSchedule(UUID clusterId, String collectionId){
        if(clusterId == null) throw new NullArgumentException("");

        try {
            /** 컬렉션의 스케쥴이 enabled 이면 다시 스케쥴을 등록한다. */
            Collection collection = collectionService.findById(clusterId, collectionId);

            try {
                if(collection.isScheduled()) {
                    String crons = collection.getCron();
                    registerCronJob(clusterId, collection.getId(), crons);
                }
            } catch (Exception e) {
                logger.error("[Register schedule ERROR] ", e);
            }
        } catch (Exception e) {
            logger.error("[Register schedule ERROR]", e);
        }
    }

    /**
     * 스케줄이 제대로 등록되어 있는지 확인하는 함수
     * @param key
     * @return
     */
    public boolean isAliveSchedule(String key){
        if(schedulerMap.get(key) == null){
            return false;
        }
        return true;
    }

    /**
     * 모든 스케줄을 읽는다.
     * @return
     */
    public List<String> getScheduledJobs(){
        List<String> result = new ArrayList<>();

        for (String key : schedulerMap.keySet() ){
            result.add(key);
        }

        return result;
    }

    /**
     * 컬렉션 스케줄에 대해서 실질적으로 스케줄 매니저를 등록한다
     * @param clusterId
     * @param collectionId
     * @param crons
     * @throws CronParseException
     */
    public void removeSchedule(UUID clusterId, String collectionId, String crons) throws CronParseException {
        if(clusterId == null || collectionId == null || collectionId.equals("") || crons == null) throw new NullArgumentException("");

        String[] cronList = collectionService.getCronList(crons);

        for(String cron : cronList){
            String scheduledKey = makeScheduleKey(clusterId, collectionId, cron);
            removeCronJob(scheduledKey);
        }
    }

    /**
     * 스케줄 키 생성 함수
     * @param clusterId
     * @param collectionId
     * @param cron
     * @return
     */
    private String makeScheduleKey(UUID clusterId, String collectionId, String cron){
        return String.format("%s-%s-%s", clusterId.toString(), collectionId, cron);
    }

    /**
     * 크론잡 스케줄 등록
     * @param clusterId
     * @param collectionId
     * @param crons
     */
    public void registerCronJob(UUID clusterId, String collectionId, String crons) throws CronParseException {
        String[] cronList = collectionService.getCronList(crons);
        for(String cron : cronList){
            String scheduledKey = makeScheduleKey(clusterId, collectionId, cron);
            logger.info("Schedule Register  ClusterId: {}, CollectionId: {}, Cron: {}", clusterId, collectionId, cron);

            schedulerMap.put(scheduledKey, Objects.requireNonNull(scheduler.schedule(() -> {
                try {
                    // 변경사항이 있을수 있으므로, 컬렉션 정보를 새로 가져온다.
                    Collection registerCollection = collectionService.findById(clusterId, collectionId);


                    IndexingStatus indexingStatus = indexingJobManager.getScheduleQueue(registerCollection.getId());

                    if (indexingStatus != null) {
                        return;
                    }
                    Deque<IndexActionStep> nextStep = new ArrayDeque<>();
                    nextStep.add(IndexActionStep.EXPOSE);
                    IndexingStatus status = indexingJobService.indexing(clusterId, registerCollection, true, IndexActionStep.FULL_INDEX, nextStep);
                    indexingJobManager.add(registerCollection.getId(), status);
                } catch (IndexingJobFailureException | IOException e) {
                    logger.error("[INIT ERROR] ", e);
                }
            }, new CronTrigger("0 " + cron))));
        }
    }
}
