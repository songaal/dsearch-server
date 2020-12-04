package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.entity.*;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.excpetions.DuplicateException;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class CollectionService {
    private static Logger logger = LoggerFactory.getLogger(CollectionService.class);

    private ConcurrentHashMap<String, ScheduledFuture<?>> scheduled = new ConcurrentHashMap<>();
    private final String lastIndexStatusIndex = ".dsearch_last_index_status";

    private final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    private final IndexingJobService indexingJobService;
    private final ClusterService clusterService;
    private final String COLLECTION_INDEX_JSON = "collection.json";
    private final String collectionIndex;
    private final ElasticsearchFactory elasticsearchFactory;
    private final IndicesService indicesService;
    private final String indexSuffixA;
    private final String indexSuffixB;
    private final IndexingJobManager indexingJobManager;

    public CollectionService(IndexingJobService indexingJobService, ClusterService clusterService,
                             @Value("${dsearch.collection.index}") String collectionIndex,
                             @Value("${dsearch.collection.index-suffix-a}") String indexSuffixA,
                             @Value("${dsearch.collection.index-suffix-b}") String indexSuffixB,
                             ElasticsearchFactory elasticsearchFactory,
                             IndicesService indicesService,
                             IndexingJobManager indexingJobManager) {
        this.indexingJobService = indexingJobService;
        this.clusterService = clusterService;
        this.indexingJobManager = indexingJobManager;
        this.collectionIndex = collectionIndex;
        this.elasticsearchFactory = elasticsearchFactory;
        this.indicesService = indicesService;
        this.indexSuffixA = indexSuffixA.toLowerCase();
        this.indexSuffixB = indexSuffixB.toLowerCase();
        this.scheduler.initialize();
        if (indexSuffixA.equalsIgnoreCase(indexSuffixB)) {
            throw new IllegalArgumentException("Error [index-suffix-a, index-suffix-b] duplicate");
        }
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
                lastIndexResponse.getHits().forEach(documentFields -> {
                    try {
                        Map<String, Object> source = documentFields.getSourceAsMap();
                        String collectionId = (String) source.get("collectionId");
                        String jobId = String.valueOf(source.getOrDefault("jobId", ""));
                        String index = (String) source.get("index");
                        long startTime = (long) source.get("startTime");
                        IndexStep step = IndexStep.valueOf(String.valueOf(source.get("step")));
                        Collection collection = findById(cluster.getId(), collectionId);
                        Collection.Launcher launcher = collection.getLauncher();
                        IndexingStatus indexingStatus = new IndexingStatus();

                        indexingStatus.setClusterId(cluster.getId());
                        indexingStatus.setIndex(index);
                        indexingStatus.setStartTime(startTime);
                        if (launcher != null) {
                            indexingStatus.setIndexingJobId(jobId);
                            indexingStatus.setHost(launcher.getHost());
                            indexingStatus.setPort(launcher.getPort());
                        }
                        indexingStatus.setAutoRun(true);
                        indexingStatus.setCurrentStep(step);
                        indexingStatus.setNextStep(new ArrayDeque<>());
                        indexingStatus.setRetry(50);
                        indexingStatus.setCollection(collection);
                        if(indexingJobManager.findById(collectionId) == null) {
                            indexingJobManager.add(collectionId, indexingStatus, false);
                            logger.debug("collection register job: {}", collectionId);
                        }
                    } catch (Exception e) {
                        logger.error("[INIT ERROR] ", e);
                    }
                });
            } catch (Exception e) {
                logger.error("[INIT ERROR]", e);
            }

            try {
//                2. 컬렉션의 스케쥴이 enabled 이면 다시 스케쥴을 등록한다.
                List<Collection> collectionList = findAll(cluster.getId());
                if (collectionList != null) {
                    collectionList.forEach(collection -> {
                        try {
                            if(collection.isScheduled()) {
                                String cron = collection.getCron();
                                String scheduledKey = String.format("%s-%s", cluster.getId(), collection.getId());
                                scheduled.put(scheduledKey, Objects.requireNonNull(scheduler.schedule(() -> {
                                    try {
                                        IndexingStatus indexingStatus = indexingJobManager.findById(collection.getId());
                                        if (indexingStatus != null) {
                                            return;
                                        }
                                        Deque<IndexStep> nextStep = new ArrayDeque<>();
                                        nextStep.add(IndexStep.PROPAGATE);
                                        nextStep.add(IndexStep.EXPOSE);
                                        IndexingStatus status = indexingJobService.indexing(cluster.getId(), collection, true, IndexStep.FULL_INDEX, nextStep);
                                        indexingJobManager.add(collection.getId(), status);
                                        logger.debug("enabled scheduled collection: {}", collection.getId());
                                    } catch (IndexingJobFailureException e) {
                                        logger.error("[INIT ERROR] ", e);
                                    }
                                }, new CronTrigger("0 " + cron))));

                            }
                        } catch (Exception e) {
                            logger.error("[INIT ERROR] ", e);
                        }
                    });
                }
            } catch (Exception e) {
                logger.error("[INIT ERROR]", e);
            }

            logger.debug("init finished");
        }
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, collectionIndex, COLLECTION_INDEX_JSON);
    }

    public void add(UUID clusterId, Collection collection) throws IOException, DuplicateException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest().indices(collectionIndex);

            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder = boolQueryBuilder.minimumShouldMatch(1);
            List<QueryBuilder> list = boolQueryBuilder.should();
//            list.add(QueryBuilders.termQuery("baseId", collection.getBaseId()));
            list.add(QueryBuilders.termQuery("baseId.keyword", collection.getBaseId()));

            searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));

            //추후 변경 예정
//            searchRequest.source(new SearchSourceBuilder().query(new TermQueryBuilder("baseId", collection.getBaseId())));
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            if (searchResponse.getHits().getTotalHits().value > 0) {
                throw new DuplicateException("duplicate baseId");
            }

            String indexNameA = collection.getBaseId() + indexSuffixA;
            String indexNameB = collection.getBaseId() + indexSuffixB;

            Collection.Index indexA = new Collection.Index();
            Collection.Index indexB = new Collection.Index();
            indexA.setIndex(indexNameA);
            indexB.setIndex(indexNameB);
            collection.setIndexA(indexA);
            collection.setIndexB(indexB);
            client.index(new IndexRequest()
                            .index(collectionIndex)
                            .source(build(collection))
                    , RequestOptions.DEFAULT);

            client.indices()
                    .putTemplate(new PutIndexTemplateRequest(collection.getBaseId()).patterns(Arrays.asList(indexNameA, indexNameB)), RequestOptions.DEFAULT);

//            client.indices().putTemplate(new PutIndexTemplateRequest(indexNameA).patterns(Arrays.asList(indexNameA)), RequestOptions.DEFAULT);
//            client.indices().putTemplate(new PutIndexTemplateRequest(indexNameB).patterns(Arrays.asList(indexNameB)), RequestOptions.DEFAULT);
        }
    }

    public List<Collection> findAll(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request indicesRequest = new Request("GET", "/_cat/indices");
            indicesRequest.addParameter("format", "json");
            Response indicesResponse = client.getLowLevelClient().performRequest(indicesRequest);
            List indexEntityList = new Gson().fromJson(EntityUtils.toString(indicesResponse.getEntity()), List.class);
            Map<String, Collection.Index> entryMap = new HashMap<>();
            for (int i = 0; i < indexEntityList.size(); i++) {
                Collection.Index tmpIndex = new Collection.Index();
                tmpIndex.setIndex(String.valueOf(((Map) indexEntityList.get(i)).get("index")));
                tmpIndex.setDocsCount(String.valueOf(((Map) indexEntityList.get(i)).get("docs.count")));
                tmpIndex.setDocsDeleted(String.valueOf(((Map) indexEntityList.get(i)).get("docs.deleted")));
                tmpIndex.setHealth(String.valueOf(((Map) indexEntityList.get(i)).get("health")));
                tmpIndex.setPri(String.valueOf(((Map) indexEntityList.get(i)).get("pri")));
                tmpIndex.setRep(String.valueOf(((Map) indexEntityList.get(i)).get("rep")));
                tmpIndex.setUuid(String.valueOf(((Map) indexEntityList.get(i)).get("uuid")));
                tmpIndex.setStoreSize(String.valueOf(((Map) indexEntityList.get(i)).get("store.size")));
                tmpIndex.setPriStoreSize(String.valueOf(((Map) indexEntityList.get(i)).get("pri.store.size")));
                entryMap.put(tmpIndex.getIndex(), tmpIndex);
            }

            Request aliasRequest = new Request("GET", "/_alias");
            aliasRequest.addParameter("format", "json");
            Response aliasResponse = client.getLowLevelClient().performRequest(aliasRequest);
            Map<String, Object> aliasEntity = new Gson().fromJson(EntityUtils.toString(aliasResponse.getEntity()), Map.class);

            List<Collection> collectionList = new ArrayList<>();
            SearchResponse response = client.search(new SearchRequest()
                            .indices(collectionIndex)
                            .source(new SearchSourceBuilder()
                                    .query(new MatchAllQueryBuilder()).sort("_id", SortOrder.DESC)
                                    .from(0)
                                    .size(10000)),
                    RequestOptions.DEFAULT);

            SearchHit[] SearchHitArr = response.getHits().getHits();
            int hitsSize = SearchHitArr.length;
            for (int i = 0; i < hitsSize; i++) {
                Map<String, Object> source = SearchHitArr[i].getSourceAsMap();
                Collection collection = convertMapToObject(SearchHitArr[i].getId(), source);
                Collection.Index indexA = entryMap.get(collection.getIndexA().getIndex());
                Collection.Index indexB = entryMap.get(collection.getIndexB().getIndex());
                if (indexA != null) {
                    indexA.setAliases((Map) ((Map) aliasEntity.get(indexA.getIndex())).get("aliases"));
                    collection.setIndexA(indexA);
                } else {
                    collection.getIndexA().setAliases(new HashMap<>());
                }
                if (indexB != null) {
                    indexB.setAliases((Map) ((Map) aliasEntity.get(indexB.getIndex())).get("aliases"));
                    collection.setIndexB(indexB);
                } else {
                    collection.getIndexB().setAliases(new HashMap<>());
                }
                collectionList.add(collection);
            }
            return collectionList;
        }
    }

    private Collection convertMapToObject(String id, Map<String, Object> source) {
        if (source == null) {
            return null;
        }
        Collection collection = new Collection();
        collection.setId(id);
        collection.setName(String.valueOf(source.get("name")));
        collection.setBaseId(String.valueOf(source.get("baseId")));
        collection.setCron(String.valueOf(source.get("cron")));
        collection.setSourceName(String.valueOf(source.get("sourceName")));
        collection.setAutoRun(Boolean.valueOf(String.valueOf(source.get("autoRun"))));

        Collection.Index indexA = new Collection.Index();
        indexA.setIndex(String.valueOf(source.get("indexA")));
        collection.setIndexA(indexA);

        Collection.Index indexB = new Collection.Index();
        indexB.setIndex(String.valueOf(source.get("indexB")));
        collection.setIndexB(indexB);

        collection.setJdbcId(String.valueOf(source.get("jdbcId")));
        collection.setScheduled(Boolean.parseBoolean(String.valueOf(source.get("scheduled"))));
        Map<String, Object> launcherMap = ((Map<String, Object>) source.get("launcher"));
        Collection.Launcher launcher = new Collection.Launcher();
        if (launcherMap != null) {
//            launcher.setPath(String.valueOf(launcherMap.get("path")));
            launcher.setYaml(String.valueOf(launcherMap.get("yaml")));
            launcher.setHost(String.valueOf(launcherMap.get("host")));
            launcher.setPort(Integer.parseInt(String.valueOf(launcherMap.get("port"))));
        }
        collection.setLauncher(launcher);
        return collection;
    }

    private XContentBuilder build(Collection collection) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("name", collection.getName())
                .field("baseId", collection.getBaseId())
                .field("indexA", collection.getIndexA().getIndex())
                .field("indexB", collection.getIndexB().getIndex())
                .field("scheduled", collection.isScheduled())
                .field("autoRun", collection.isAutoRun())
                .field("sourceName", collection.getSourceName() == null ? "" : collection.getSourceName())
                .field("jdbcId", collection.getJdbcId() == null ? "" : collection.getJdbcId())
                .field("cron", collection.getCron() == null ? "" : collection.getCron());
        if (collection.getLauncher() != null) {
            Collection.Launcher launcher = collection.getLauncher();
            builder.startObject("launcher")
//                    .field("path", launcher.getPath() == null ? "" : launcher.getPath())
                    .field("yaml", launcher.getYaml() == null ? "" : launcher.getYaml())
                    .field("host", launcher.getHost() == null ? "" : launcher.getHost())
                    .field("port", launcher.getPort() == 0 ? "" : launcher.getPort())
                    .endObject();
        }
        return builder.endObject();
    }

    public Collection findById(UUID clusterId, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(new GetRequest().index(collectionIndex).id(id), RequestOptions.DEFAULT);
            Collection collection = convertMapToObject(getResponse.getId(), getResponse.getSourceAsMap());
            collection.setIndexA(getIndex(clusterId, collection.getIndexA().getIndex()));
            collection.setIndexB(getIndex(clusterId, collection.getIndexB().getIndex()));

            if (collection.getIndexA().getUuid() != null) {
                collection.getIndexA().setAliases(getAlias(clusterId, collection.getIndexA().getIndex()));
            } else {
                collection.getIndexA().setAliases(new HashMap<>());
            }
            if (collection.getIndexB().getUuid() != null) {
                collection.getIndexB().setAliases(getAlias(clusterId, collection.getIndexB().getIndex()));
            } else {
                collection.getIndexB().setAliases(new HashMap<>());
            }
            return collection;
        }
    }
    private Map getAlias(UUID clusterId, String index) {
        Map result = null;
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request aliasRequest = new Request("GET", "/" + index +"/_alias");
            aliasRequest.addParameter("format", "json");
            Response aliasResponse = client.getLowLevelClient().performRequest(aliasRequest);
            Map<String, Object> aliasEntity = new Gson().fromJson(EntityUtils.toString(aliasResponse.getEntity()), Map.class);
            result = (Map) ((Map)aliasEntity.get(index)).get("aliases");
        } catch (IOException e) {
            logger.debug("NotFoundAlias: {}", index);
        }
        return result;
    }

    private Collection.Index getIndex(UUID clusterId, String index) {
        Collection.Index tmpIndex = new Collection.Index();
        tmpIndex.setIndex(index);
//        logger.debug("getIndex >>> {}", index);
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request indicesRequest = new Request("GET", "/_cat/indices/" + index);
            indicesRequest.addParameter("format", "json");
            Response indicesResponse = client.getLowLevelClient().performRequest(indicesRequest);
            List indexEntityList = new Gson().fromJson(EntityUtils.toString(indicesResponse.getEntity()), List.class);
            if (indexEntityList.size() == 1) {
                tmpIndex.setDocsCount(String.valueOf(((Map) indexEntityList.get(0)).get("docs.count")));
                tmpIndex.setDocsDeleted(String.valueOf(((Map) indexEntityList.get(0)).get("docs.deleted")));
                tmpIndex.setHealth(String.valueOf(((Map) indexEntityList.get(0)).get("health")));
                tmpIndex.setPri(String.valueOf(((Map) indexEntityList.get(0)).get("pri")));
                tmpIndex.setRep(String.valueOf(((Map) indexEntityList.get(0)).get("rep")));
                tmpIndex.setUuid(String.valueOf(((Map) indexEntityList.get(0)).get("uuid")));
                tmpIndex.setStoreSize(String.valueOf(((Map) indexEntityList.get(0)).get("store.size")));
                tmpIndex.setPriStoreSize(String.valueOf(((Map) indexEntityList.get(0)).get("pri.store.size")));
            }
        } catch (IOException e) {
            logger.warn("NotFoundIndex: {}", index);
        }
        return tmpIndex;
    }

    public void deleteById(UUID clusterId, String id) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection collection = findById(clusterId, id);
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

            if (collection.isScheduled()) {
                try {
                    collection.setScheduled(false);
                    editSchedule(clusterId, id, collection);
                } catch (Exception e){
                    logger.error("", e);
                }
            }
            if (indexA.getUuid() != null) {
                try {
                    client.delete(new DeleteRequest().index(indexA.getIndex()), RequestOptions.DEFAULT);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            if (indexB.getUuid() != null) {
                try {
                    client.delete(new DeleteRequest().index(indexB.getIndex()), RequestOptions.DEFAULT);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }

            try {
                client.indices().delete(new DeleteIndexRequest(indexA.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }
            try {
                client.indices().delete(new DeleteIndexRequest(indexB.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }
            try {
                client.indices().deleteTemplate(new DeleteIndexTemplateRequest(indexA.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }
            try {
                client.indices().deleteTemplate(new DeleteIndexTemplateRequest(indexB.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }
            try {
                client.delete(new DeleteRequest().index(collectionIndex).id(id), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
    }


    public void editSource(UUID clusterId, String id, Collection collection) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(new GetRequest().index(collectionIndex).id(id), RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            if(collection.getName() != null && collection.getName().length() > 0)
                sourceAsMap.put("name", collection.getName());
            sourceAsMap.put("cron", collection.getCron());
            sourceAsMap.put("sourceName", collection.getSourceName());
            sourceAsMap.put("jdbcId", collection.getJdbcId());
            sourceAsMap.put("refresh_interval", collection.getRefresh_interval());
            sourceAsMap.put("replicas", collection.getReplicas());
            Map<String, Object> launcherSourceAsMap = (Map<String, Object>) sourceAsMap.get("launcher");
            if (launcherSourceAsMap == null) {
                launcherSourceAsMap = new HashMap<>();
            }
            launcherSourceAsMap.put("yaml", collection.getLauncher().getYaml());
            launcherSourceAsMap.put("host", collection.getLauncher().getHost());
            launcherSourceAsMap.put("port", collection.getLauncher().getPort());
            sourceAsMap.put("launcher", launcherSourceAsMap);

            UpdateResponse updateResponse = client.update(new UpdateRequest().index(collectionIndex).
                    id(id).
                    doc(sourceAsMap), RequestOptions.DEFAULT);

            logger.debug("update Response: {}", updateResponse);
        }
    }

    public void editSchedule(UUID clusterId, String id, Collection collection) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(new GetRequest().index(collectionIndex).id(id), RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            sourceAsMap.put("scheduled", collection.isScheduled());
            client.update(new UpdateRequest()
                    .index(collectionIndex)
                    .id(id)
                    .doc(sourceAsMap), RequestOptions.DEFAULT);
            Collection registerCollection = findById(clusterId, id);

            String scheduledKey = String.format("%s-%s", clusterId.toString(), registerCollection.getId());
            if (registerCollection.isScheduled()) {
                String cron = registerCollection.getCron();
                scheduled.put(scheduledKey, Objects.requireNonNull(scheduler.schedule(() -> {
                    try {
                        IndexingStatus indexingStatus = indexingJobManager.findById(registerCollection.getId());
                        if (indexingStatus != null) {
                            return;
                        }
                        Deque<IndexStep> nextStep = new ArrayDeque<>();
                        nextStep.add(IndexStep.PROPAGATE);
                        nextStep.add(IndexStep.EXPOSE);
                        IndexingStatus status = indexingJobService.indexing(clusterId, registerCollection, true, IndexStep.FULL_INDEX, nextStep);
                        indexingJobManager.add(registerCollection.getId(), status);
                    } catch (IndexingJobFailureException e) {
                        logger.error("", e);
                    }
                }, new CronTrigger("0 " + cron))));
            } else {
                ScheduledFuture<?> future = scheduled.get(scheduledKey);
                if (future != null) {
                    try {
                        future.cancel(true);
                        scheduled.remove(collection.getId());
                        logger.debug("collection {} >> cancel {}", collection.getId(), future.isCancelled());
                    } catch (NullPointerException e) {
                        logger.warn("ignore exception", e.getMessage());
                    }
                }
            }
        }
    }

    public Collection findByName(UUID clusterId, String name) throws IOException {
        List<Collection> list = findAll(clusterId);
        Collection result = null;
        for(Collection collection : list){
            if(collection.getBaseId().equals(name)){
                result = collection;
                break;
            }
        }
        return result;
    }
}
