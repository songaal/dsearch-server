package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.collections.entity.IndexStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.excpetions.CronParseException;
import com.danawa.dsearch.server.excpetions.DuplicatedUserException;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.apache.commons.lang.NullArgumentException;
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

    private void removeSchedule(UUID clusterId, String collectionId){
        String prefix = String.format("%s-%s", clusterId, collectionId);
        Iterator<String> keys = scheduled.keySet().iterator();
        while (keys.hasNext()){
            String key = keys.next();
            if(key.contains(prefix)){
                logger.info("Schedule Removed key : {}", key);
                ScheduledFuture<?> future = scheduled.get(key);
                future.cancel(true);
                scheduled.remove(key);
            }
        }
    }

    private void registerSchedule(UUID clusterId, String collectionId, String crons){
        String[] cronList = crons.split("\\|\\|");
        for(String cron : cronList){
            String scheduledKey = String.format("%s-%s-%s", clusterId, collectionId, cron);
            logger.info("Schedule Register  ClusterId: {}, CollectionId: {}, Cron: {}", clusterId, collectionId, cron);

            scheduled.put(scheduledKey, Objects.requireNonNull(scheduler.schedule(() -> {
                try {
                    // 변경사항이 있을수 있으므로, 컬렉션 정보를 새로 가져온다.
                    Collection registerCollection = findById(clusterId, collectionId);
                    logger.info("{}", registerCollection.getIndexingType());
                    IndexingStatus indexingStatus = indexingJobManager.findById(registerCollection.getId());
                    if (indexingStatus != null) {
                        return;
                    }
                    Deque<IndexStep> nextStep = new ArrayDeque<>();
                    nextStep.add(IndexStep.EXPOSE);
                    IndexingStatus status;
                    status = indexingJobService.indexing(clusterId, registerCollection, true, IndexStep.FULL_INDEX, nextStep);
                    indexingJobManager.add(registerCollection.getId(), status);
                    logger.debug("enabled scheduled collection: {}", registerCollection.getId());
                } catch (IndexingJobFailureException | IOException e) {
                    logger.error("[INIT ERROR] ", e);
                }
            }, new CronTrigger("0 " + cron))));
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
                            IndexStep step = IndexStep.valueOf(String.valueOf(source.get("step")));

                            if (expireStartTime <= startTime) {
                                Collection collection = findById(cluster.getId(), collectionId);
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
                                if(indexingJobManager.findById(collectionId) == null) {
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

            try {
//              컬렉션의 스케쥴이 enabled 이면 다시 스케쥴을 등록한다.
                List<Collection> collectionList = findAll(cluster.getId());
                if (collectionList != null) {
                    collectionList.forEach(collection -> {
                        try {
                            if(collection.isScheduled()) {
                                registerSchedule(cluster.getId(), collection.getId(), collection.getCron());
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

    public void add(UUID clusterId, Collection collection) throws IOException, DuplicatedUserException {
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
                throw new DuplicatedUserException("duplicate baseId");
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
        }
    }

    public List<Collection> findAll(UUID clusterId) throws IOException {
        if(clusterId == null) throw new NullArgumentException("");

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
        if(source.get("replicas") != null){
            collection.setReplicas(Integer.parseInt(String.valueOf(source.get("replicas"))));
        }else{
            collection.setReplicas(1);
        }
        if(source.get("refresh_interval") != null){
            collection.setRefresh_interval(Integer.parseInt(String.valueOf(source.get("refresh_interval"))));
        }else{
            collection.setRefresh_interval(1);
        }
        if(source.get("ignoreRoleYn") != null){
            collection.setIgnoreRoleYn(String.valueOf(source.get("ignoreRoleYn")));
        }else{
            collection.setIgnoreRoleYn("N");
        }

        collection.setCron(String.valueOf(source.get("cron")));
        collection.setSourceName(String.valueOf(source.get("sourceName")));
        collection.setAutoRun(Boolean.parseBoolean(String.valueOf(source.get("autoRun"))));
        collection.setEsScheme(String.valueOf(source.get("esScheme")));
        collection.setEsHost(String.valueOf(source.get("esHost")));
        collection.setEsPort(String.valueOf(source.get("esPort")));
        collection.setEsUser(String.valueOf(source.get("esUser")));
        collection.setEsPassword(String.valueOf(source.get("esPassword")));
        // 21.09.07
        collection.setIndexingType(String.valueOf(source.get("indexingType")));

        collection.setExtIndexer(Boolean.parseBoolean(String.valueOf(source.get("extIndexer"))));

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
            launcher.setScheme(String.valueOf(launcherMap.get("scheme")));
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
                .field("cron", collection.getCron() == null ? "" : collection.getCron())

                .field("esScheme", collection.getEsScheme() == null ? "" : collection.getEsScheme())
                .field("esHost", collection.getEsHost() == null ? "" : collection.getEsHost())
                .field("esPort", collection.getEsPort() == null ? "" : collection.getEsPort())
                .field("esUser", collection.getEsUser() == null ? "" : collection.getEsUser())
                .field("esPassword", collection.getEsPassword() == null ? "" : collection.getEsPassword())
                .field("extIndexer", collection.isExtIndexer());
        if (collection.getLauncher() != null) {
            Collection.Launcher launcher = collection.getLauncher();
            builder.startObject("launcher")
                    .field("scheme", launcher.getScheme() == null ? "" : launcher.getScheme())
                    .field("yaml", launcher.getYaml() == null ? "" : launcher.getYaml())
                    .field("host", launcher.getHost() == null ? "" : launcher.getHost())
                    .field("port", launcher.getPort() == 0 ? "" : launcher.getPort())
                    .endObject();
        }
        return builder.endObject();
    }

    public Collection findById(UUID clusterId, String id) throws IOException {
        if(clusterId == null || id == null || id.equals("")) throw new NullArgumentException("");

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

            collection = tmpFillData(collection);

            return collection;
        }
    }

    private Collection tmpFillData(Collection collection) {
        // 2021.02.08 임시 사용 - 운영 데이터에 필드 적용 후 삭제예정
        if( collection.getEsHost() == null || "".equals(collection.getEsHost()) || "null".equals(collection.getEsHost()) ) {
            try {
                collection.setExtIndexer(true);
                collection.getLauncher().setScheme("http");
                Map<String ,Object> yamlToMap = indexingJobService.convertRequestParams(collection.getLauncher().getYaml());
                collection.setEsScheme((String) yamlToMap.get("scheme"));
                collection.setEsHost((String) yamlToMap.get("host"));
                collection.setEsPort(String.valueOf(yamlToMap.get("port")));
                collection.setEsUser("");
                collection.setEsPassword("");
            } catch (Exception e) {
                logger.warn("", e);
            }
        }
        return collection;
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
        if(clusterId == null || id == null || id.equals("")) throw new NullArgumentException("");

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

    public void editSource(UUID clusterId, String id, Collection collection) throws IOException, CronParseException {
        if(clusterId == null || id == null || id.equals("") || collection == null) throw new NullArgumentException("");

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(new GetRequest().index(collectionIndex).id(id), RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            boolean isScheduled = (boolean) sourceAsMap.get("scheduled");

            if(collection.getName() != null && collection.getName().length() > 0)
                sourceAsMap.put("name", collection.getName());
            sourceAsMap.put("cron", collection.getCron());
            sourceAsMap.put("sourceName", collection.getSourceName());
            sourceAsMap.put("jdbcId", collection.getJdbcId());
            sourceAsMap.put("refresh_interval", collection.getRefresh_interval());
            sourceAsMap.put("replicas", collection.getReplicas());
            sourceAsMap.put("ignoreRoleYn", collection.getIgnoreRoleYn());
            sourceAsMap.put("extIndexer", collection.isExtIndexer());

            if (collection.getEsHost() != null && !"".equals(collection.getEsHost())
                    && collection.getEsPort() != null && !"".equals(collection.getEsPort())) {
                sourceAsMap.put("esScheme", collection.getEsScheme());
                sourceAsMap.put("esHost", collection.getEsHost());
                sourceAsMap.put("esPort", collection.getEsPort());
                sourceAsMap.put("esUser", collection.getEsUser());
                sourceAsMap.put("esPassword", collection.getEsPassword());
            } else {
                Cluster cluster = clusterService.find(clusterId);
                sourceAsMap.put("esScheme", cluster.getScheme());
                sourceAsMap.put("esHost", cluster.getHost());
                sourceAsMap.put("esPort", cluster.getPort());
                sourceAsMap.put("esUser", cluster.getUsername());
                sourceAsMap.put("esPassword", cluster.getPassword());
            }

            logger.debug("collection 내용 : {}", collection);

            Map<String, Object> launcherSourceAsMap = (Map<String, Object>) sourceAsMap.get("launcher");
            if (launcherSourceAsMap == null) {
                launcherSourceAsMap = new HashMap<>();
            }
            launcherSourceAsMap.put("yaml", collection.getLauncher().getYaml());
            launcherSourceAsMap.put("scheme", collection.getLauncher().getScheme());
            launcherSourceAsMap.put("host", collection.getLauncher().getHost());
            launcherSourceAsMap.put("port", collection.getLauncher().getPort());
            sourceAsMap.put("launcher", launcherSourceAsMap);

            UpdateResponse updateResponse = client.update(new UpdateRequest().index(collectionIndex).
                    id(id).
                    doc(sourceAsMap), RequestOptions.DEFAULT);

            logger.debug("update Response: {}", updateResponse);

            // 스케줄이 있다면
            if(isScheduled){
                collection.setScheduled(true); // collection에는 따로 셋팅해서 넘겨준다. -> sourceAsMap에는 있지만 Collection에는 false로 등록되어 있을 수 있기 때문에.
                removeSchedule(clusterId, id); // 기존 스케줄을 전부 지우고
                editSchedule(clusterId, id, collection); //새로 스케줄을 등록한다
            }
        }
    }

    public void editSchedule(UUID clusterId, String id, Collection collection) throws IOException, CronParseException {
        if(clusterId == null || id == null || id.equals("") || collection == null) throw new NullArgumentException("");
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(new GetRequest().index(collectionIndex).id(id), RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            sourceAsMap.put("scheduled", collection.isScheduled());
            client.update(new UpdateRequest()
                    .index(collectionIndex)
                    .id(id)
                    .doc(sourceAsMap), RequestOptions.DEFAULT);
            Collection registerCollection = findById(clusterId, id);

            String crons = registerCollection.getCron();
            if (registerCollection.isScheduled()) {
                registerSchedule(clusterId, registerCollection.getId(), crons);
            }else{
                removeSchedule(clusterId, registerCollection.getId(), crons);
            }
        }
    }

    public Collection findByName(UUID clusterId, String name) throws IOException {
        if(clusterId == null || name == null || name.equals("") ) throw new NullArgumentException("");

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

    public void removeAllSchedule(UUID clusterId){
        if(clusterId == null) throw new NullArgumentException("");

        for(String key : scheduled.keySet()){
            if(key.contains(clusterId.toString())){
                logger.info("스케줄 제거, clusterId: {}, scheduled key: {}", clusterId, key);
                scheduled.get(key).cancel(true);
                scheduled.remove(key);
            }
        }
    }

    public void registerAllSchedule(UUID clusterId){
        if(clusterId == null) throw new NullArgumentException("");

        try {
           /** 컬렉션의 스케쥴이 enabled 이면 다시 스케쥴을 등록한다. */
            List<Collection> collectionList = findAll(clusterId);
            if (collectionList != null) {
                collectionList.forEach(collection -> {
                    try {
                        if(collection.isScheduled()) {
                            String crons = collection.getCron();
                            registerSchedule(clusterId, collection.getId(), crons);
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

    public void removeSchedule(UUID clusterId, String collectionId, String crons ) throws CronParseException {
        if(clusterId == null || collectionId == null || collectionId.equals("") || crons == null) throw new NullArgumentException("");

        String[] cronList = crons.split("\\|\\|");
        if(cronList.length > 0){
            String[] splitted = cronList[0].split(" ");
            if(splitted.length != 5){
                throw new CronParseException("");
            }
        }

        for(String cron : cronList){
            String scheduledKey = String.format("%s-%s-%s", clusterId.toString(), collectionId, cron);
            ScheduledFuture<?> future = scheduled.get(scheduledKey);
            if (future != null) {
                logger.info("Remove Scheduling.. scheduledKey: {}, future: {}", scheduledKey, future);
                try {
                    future.cancel(true);
                    scheduled.remove(collectionId);
                    logger.debug("collection {} >> cancel {}", collectionId, future.isCancelled());
                } catch (NullPointerException e) {
                    logger.warn("ignore exception {}", e.getMessage());
                }
            }
        }
    }

    public String download(UUID clusterId, Map<String, Object> message){
        if(clusterId == null || message == null) throw new NullArgumentException("");

        StringBuffer sb = new StringBuffer();
        Map<String, Object> collection = new HashMap<>();
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(collectionIndex).source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(10000).from(0));
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] hits = response.getHits().getHits();

            List<String> list = new ArrayList<>();

            Gson gson = JsonUtils.createCustomGson();
            int count = 0;
            for(SearchHit hit : hits){
                if(count != 0){
                    sb.append(",\n");
                }
                Map<String, Object> body = new HashMap<>();
                body.put("_index", collectionIndex);
                body.put("_type", "_doc");
                body.put("_id", hit.getId());
                body.put("_score", hit.getScore());
                body.put("_source", hit.getSourceAsMap());
                list.add(hit.getSourceAsMap().get("baseId") + "");
//                list.add(hit.getSourceAsMap().get("name") + " [" + hit.getSourceAsMap().get("baseId") + "]");
                String stringBody = gson.toJson(body);
                sb.append(stringBody);
                count++;
            }
            collection.put("result", true);
            collection.put("count", hits.length);
            collection.put("list", list);

        } catch (IOException e) {
            collection.put("result", false);
            collection.put("count", 0);
            collection.put("message", e.getMessage());
            collection.put("list", new ArrayList<>());
            logger.error("{}", e);
        }
        message.put("collection", collection);
        return sb.toString();
    }
}
