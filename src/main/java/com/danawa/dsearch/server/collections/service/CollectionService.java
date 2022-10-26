package com.danawa.dsearch.server.collections.service;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingActionType;
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@Service
public class CollectionService {
    private static Logger logger = LoggerFactory.getLogger(CollectionService.class);
    private static Object obj = new Object();

    private final IndexingJobService indexingJobService;
    private final ClusterService clusterService;
    private final String COLLECTION_INDEX_JSON = "collection.json";
    private final String collectionIndex;
    private final ElasticsearchFactory elasticsearchFactory;
    private final IndicesService indicesService;
    private final String indexSuffixA;
    private final String indexSuffixB;
    private final IndexingJobManager indexingJobManager;
    public CollectionService(ClusterService clusterService,
                             @Value("${dsearch.collection.index}") String collectionIndex,
                             @Value("${dsearch.collection.index-suffix-a}") String indexSuffixA,
                             @Value("${dsearch.collection.index-suffix-b}") String indexSuffixB,
                             ElasticsearchFactory elasticsearchFactory,
                             IndicesService indicesService,
                             IndexingJobService indexingJobService,
                             IndexingJobManager indexingJobManager
    ) {
        this.indexingJobService = indexingJobService;
        this.clusterService = clusterService;
        this.indexingJobManager = indexingJobManager;
        this.collectionIndex = collectionIndex;
        this.elasticsearchFactory = elasticsearchFactory;
        this.indicesService = indicesService;
        this.indexSuffixA = indexSuffixA.toLowerCase();
        this.indexSuffixB = indexSuffixB.toLowerCase();

        if (indexSuffixA.equalsIgnoreCase(indexSuffixB)) {
            throw new IllegalArgumentException("Error [index-suffix-a, index-suffix-b] duplicate");
        }
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, collectionIndex, COLLECTION_INDEX_JSON);
    }

    public void create(UUID clusterId, Collection collection) throws IOException, DuplicatedUserException {
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            SearchRequest searchRequest = new SearchRequest().indices(collectionIndex);

            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            boolQueryBuilder = boolQueryBuilder.minimumShouldMatch(1);
            List<QueryBuilder> list = boolQueryBuilder.should();
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

            AcknowledgedResponse response = client.indices()
                    .putTemplate(new PutIndexTemplateRequest(collection.getBaseId()).patterns(Arrays.asList(indexNameA, indexNameB)), RequestOptions.DEFAULT);
            if(response.isAcknowledged()){
                logger.info("collection [{}] is created", collection.getBaseId());
            }
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
                    updateSchedule(clusterId, id, collection);
                } catch (Exception e){
                    logger.error("", e);
                }
            }
            if (indexA.getUuid() != null) {
                try {
                    logger.info("clusterId={}, id={}, delete index={}", clusterId, id, indexA.getIndex());
                    client.delete(new DeleteRequest().index(indexA.getIndex()), RequestOptions.DEFAULT);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
            if (indexB.getUuid() != null) {
                try {
                    logger.info("clusterId={}, id={}, delete index={}", clusterId, id, indexB.getIndex());
                    client.delete(new DeleteRequest().index(indexB.getIndex()), RequestOptions.DEFAULT);
                } catch (Exception e) {
                    logger.error("", e);
                }
            }

            try {
                logger.info("clusterId={}, id={}, delete index={}", clusterId, id, indexA.getIndex());
                client.indices().delete(new DeleteIndexRequest(indexA.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }
            try {
                logger.info("clusterId={}, id={}, delete index={}", clusterId, id, indexB.getIndex());
                client.indices().delete(new DeleteIndexRequest(indexB.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }

            try {
                logger.info("clusterId={}, id={}, delete index={}", clusterId, id, indexA.getIndex());
                client.indices().deleteTemplate(new DeleteIndexTemplateRequest(indexA.getIndex()), RequestOptions.DEFAULT);
            } catch (Exception e) {
                logger.warn("", e);
            }

            try {
                logger.info("clusterId={}, id={}, delete index={}", clusterId, id, indexB.getIndex());
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

    public Map<String, Object> updateSource(UUID clusterId, String collectionId, Collection collection) throws IOException {
        if(clusterId == null || collectionId == null || collectionId.equals("") || collection == null) throw new NullArgumentException("");

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(new GetRequest().index(collectionIndex).id(collectionId), RequestOptions.DEFAULT);
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            if(collection.getName() != null && collection.getName().length() > 0) {
                sourceAsMap.put("name", collection.getName());
            }

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
                    id(collectionId).
                    doc(sourceAsMap), RequestOptions.DEFAULT);

            logger.debug("update Response: {}", updateResponse);
            return sourceAsMap;
        }
    }

    public void updateSchedule(UUID clusterId, String collectionId, Collection collection) throws IOException {
        if(clusterId == null || collectionId == null || collectionId.equals("") || collection == null) throw new NullArgumentException("");

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            GetResponse getResponse = client.get(
                    new GetRequest().index(collectionIndex).id(collectionId), RequestOptions.DEFAULT);

            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            sourceAsMap.put("scheduled", collection.isScheduled());
            client.update(new UpdateRequest()
                    .index(collectionIndex)
                    .id(collectionId)
                    .doc(sourceAsMap), RequestOptions.DEFAULT);
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


    /**
     * 크론을 문자열 형태로 입력받아서 파싱하고, 파싱된 내용을 검사하여 검사 통과를 못하면 Exception을 발생시킨다
     * @param crons
     * @return cronList
     * @throws CronParseException
     */
    public String[] getCronList(String crons) throws CronParseException {
        String[] cronList = crons.split("\\|\\|");

        if(cronList.length > 0){
            for(String cron : cronList){
                String[] splitted = cron.split(" ");

                if(splitted.length != 5){
                    throw new CronParseException("크론을 파싱했으나 정상적으로 파싱 되지 않음");
                }
            }
        }
        return cronList;
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

    public void processIndexingJob(
            UUID clusterId,
            String clientIP,
            String id,
            Collection collection,
            IndexingActionType actionType,
            String groupSeq,
            Map<String, Object> response) {

        logger.info("clusterId={}, clientIP={}, id={}, collection={}, actionType={}, groupSeq={}, response={}",
                clusterId,
                clientIP,
                id,
                collection,
                actionType,
                groupSeq,
                response);

        switch (actionType){
            case ALL:
                synchronized (obj) {
                    try{
                        IndexingStatus status = startIndexingAll(clusterId, collection, actionType);
                        response.put("indexingStatus", status);
                        response.put("result", "success");
                    } catch (IndexingJobFailureException e) {
                        indexingJobManager.setQueueStatus(collection.getId(), "ERROR");
                        response.put("result", "fail");
                    }
                }
                break;
            case INDEXING:
                synchronized (obj) {
                    try {
                        IndexingStatus status = startIndexing(clusterId, collection, actionType);
                        response.put("indexingStatus", status);
                        response.put("result", "success");
                    } catch (IndexingJobFailureException e) {
                        indexingJobManager.setQueueStatus(collection.getId(), "ERROR");
                        response.put("result", "fail");
                    }
                }
                break;
            case EXPOSE:
                synchronized (obj) {
                    try{
                        startExpose(clusterId, collection);
                        response.put("result", "success");
                    } catch (IOException | IndexingJobFailureException e) {
                        indexingJobManager.setQueueStatus(collection.getId(), "ERROR");
                        response.put("result", "fail");
                    }
                }
                break;
            case STOP_INDEXING:
                synchronized (obj) {
                    IndexingStatus indexingStatus = stopIndexing(collection);
                    if(indexingStatus == null){
                        response.put("message", "try to stop indexing but not found indexing...");
                    }else{
                        response.put("message", "stopped");
                        response.put("indexingStatus", indexingStatus);
                    }

                    response.put("result", "success");
                }
                break;
            case SUB_START:
                synchronized (obj) {
                    IndexingStatus status = startIndexingForSubStart(collection, groupSeq);
                    if (status == null) {
                        response.put("result", "fail");
                    } else {
                        response.put("indexingStatus", status);
                        response.put("result", "success");
                    }
                }
                break;
            default:
                response.put("message", "Not Found Action. Please Select Action in this list (all / indexing / propagate / expose / stop_indexing / stop_propagation)");
                response.put("result", "success");
        }
    }

    public IndexingStatus startIndexingAll(UUID clusterId, Collection collection, IndexingActionType actionType) throws IndexingJobFailureException {
        String collectionId = collection.getId();

        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);
        if(status != null){
            throw new IndexingJobFailureException("기존 데이터가 있음");
        }

        Queue<IndexActionStep> nextStep = new ArrayDeque<>();
        nextStep.add(IndexActionStep.EXPOSE);
        status = indexingJobService.indexing(clusterId, collection, true, IndexActionStep.FULL_INDEX, nextStep);
        status.setAction(actionType.getAction());
        status.setStatus("RUNNING");
        indexingJobManager.add(collectionId, status);
        return status;
    }

    public IndexingStatus startIndexing(UUID clusterId, Collection collection, IndexingActionType actionType) throws IndexingJobFailureException {
        String collectionId = collection.getId();

        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);
        if(status != null){
            throw new IndexingJobFailureException("기존 데이터가 있음");
        }

        status = indexingJobService.indexing(clusterId, collection, false, IndexActionStep.FULL_INDEX);
        status.setAction(actionType.getAction());
        status.setStatus("RUNNING");
        indexingJobManager.add(collection.getId(), status);
        return status;
    }

    public void startExpose(UUID clusterId, Collection collection) throws IndexingJobFailureException, IOException {
        String collectionId = collection.getId();

        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);
        if(status != null){
            throw new IndexingJobFailureException("기존 데이터가 있음");
        }
        indexingJobService.expose(clusterId, collection);
    }

    public IndexingStatus stopIndexing(Collection collection){
        String collectionId = collection.getId();
        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);

        if (isRightStatusForStopIndexing(status)) {
            indexingJobService.stopIndexing(status);
            indexingJobManager.setQueueStatus(collectionId, "STOP");
        }

        return indexingJobManager.getManageQueue(collectionId);
    }
    private boolean isRightStatusForStopIndexing(IndexingStatus status){
        return status != null
                && (status.getCurrentStep() == IndexActionStep.FULL_INDEX
                || status.getCurrentStep() == IndexActionStep.DYNAMIC_INDEX );
    }

    public IndexingStatus startIndexingForSubStart(Collection collection, String groupSeq){
        String collectionId = collection.getId();
        IndexingStatus status = indexingJobManager.getManageQueue(collectionId);

        if (isRightStatusForSubStart(status, groupSeq)) {
            logger.info("sub_start >>>> {}, groupSeq: {}", collection.getName(), groupSeq);
            indexingJobService.subStart(status, collection, groupSeq);
            return status;
        }else{
            return null;
        }
    }
    private boolean isRightStatusForSubStart(IndexingStatus status, String groupSeq){
        return status != null && (status.getCurrentStep() == IndexActionStep.FULL_INDEX || status.getCurrentStep() == IndexActionStep.DYNAMIC_INDEX) && groupSeq != null && !"".equalsIgnoreCase(groupSeq);
    }
}
