package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.IndexerConfig;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.entity.IndexStep;
import com.danawa.dsearch.server.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.fastcatx.indexer.IndexJobManager;
import com.danawa.fastcatx.indexer.entity.Job;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URI;
import java.util.*;

@Service
@ConfigurationProperties(prefix = "dsearch.collection")
public class IndexingJobService {
    private static final Logger logger = LoggerFactory.getLogger(IndexingJobService.class);
    private final ElasticsearchFactory elasticsearchFactory;
    private final RestTemplate restTemplate;

    private final String indexHistory = ".dsearch_index_history";

    private final String jdbcSystemIndex;
    private final com.danawa.fastcatx.indexer.IndexJobManager indexerJobManager;

    private Map<String, Object> params;
    private Map<String, Object> indexing;
    private Map<String, Object> propagate;

    public IndexingJobService(ElasticsearchFactory elasticsearchFactory,
                              @Value("${dsearch.jdbc.setting}") String jdbcSystemIndex,
                              IndexJobManager indexerJobManager) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.jdbcSystemIndex = jdbcSystemIndex;
        this.indexerJobManager = indexerJobManager;

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    public com.danawa.fastcatx.indexer.IndexJobManager getIndexerJobManager() {
        return this.indexerJobManager;
    }

    /**
     * 소스를 읽어들여 ES 색인에 입력하는 작업.
     * indexer를 외부 프로세스로 실행한다.
     *
     * @return IndexingStatus*/
    public IndexingStatus indexing(UUID clusterId, Collection collection, boolean autoRun, IndexStep step) throws IndexingJobFailureException {
        return indexing(clusterId, collection, autoRun, step, new ArrayDeque<>());
    }
    public IndexingStatus indexing(UUID clusterId, Collection collection, boolean autoRun, IndexStep step, Queue<IndexStep> nextStep) throws IndexingJobFailureException {
        IndexingStatus indexingStatus = new IndexingStatus();
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

//            1. 대상 인덱스 찾기.
            logger.info("{} 대상 인덱스 찾기", collection.getBaseId());
            Collection.Index index = getTargetIndex(collection.getBaseId(), indexA, indexB);

//            2. 인덱스 설정 변경.
//            editPreparations(client, index); // 이전버전
            logger.info("{} 인덱스 설정 변경", index);
            editPreparations(client, collection, index);

//            3. 런처 파라미터 변환작업
            logger.info("{} 런처 파라미터 변환 작업", index);
            Collection.Launcher launcher = collection.getLauncher();

            Map<String, Object> body = convertRequestParams(launcher.getYaml());
            if (collection.getJdbcId() != null && !"".equals(collection.getJdbcId())) {
                GetResponse getResponse = client.get(new GetRequest().index(jdbcSystemIndex).id(collection.getJdbcId()), RequestOptions.DEFAULT);
                Map<String, Object> jdbcSource = getResponse.getSourceAsMap();
                jdbcSource.put("driverClassName", jdbcSource.get("driver"));
                jdbcSource.put("url", jdbcSource.get("url"));
                jdbcSource.put("user", jdbcSource.get("user"));
                jdbcSource.put("password", jdbcSource.get("password"));
                body.put("_jdbc", jdbcSource);
            }

            body.put("index", index.getIndex());

            Map<String, Object> tmp = new HashMap<>() ;
            for(String key : indexing.keySet()){
                tmp.put(key, indexing.get(key));
            }

            if(collection.getIgnoreRoleYn() != null && collection.getIgnoreRoleYn().equals("Y")) {
                tmp.remove("index.routing.allocation.include.role");
                tmp.remove("index.routing.allocation.exclude.role");
            }else{
                tmp.replace("index.routing.allocation.include.role", "");
                tmp.replace("index.routing.allocation.exclude.role", "index");
            }

            body.put("_indexingSettings", tmp);

            body.put("scheme", collection.getEsScheme());
            body.put("host", collection.getEsHost());

            int esPort = 0;
            try{
                esPort = Integer.parseInt(collection.getEsPort());
            }catch (NumberFormatException e){
                logger.info("host: {}, port: {}, scheme {}" , collection.getEsHost(), collection.getEsPort(), collection.getEsScheme());
            }

            body.put("port", collection.getEsPort());

//            body.put("username", collection.getEsUser());
//            body.put("password", collection.getEsPassword());

            if (launcher.getScheme() == null || "".equals(launcher.getScheme())) {
                launcher.setScheme("http");
            }

            String indexingJobId;
//            4. indexer 색인 전송
            logger.debug("외부 인덱서 사용 여부 : {}", collection.isExtIndexer());
            if (collection.isExtIndexer()) {
                // 외부 인덱서를 사용할 경우 전송.
                ResponseEntity<Map> responseEntity = restTemplate.exchange(
                        new URI(String.format("%s://%s:%d/async/start", launcher.getScheme(), launcher.getHost(), launcher.getPort())),
                        HttpMethod.POST,
                        new HttpEntity(body),
                        Map.class
                );
                if (responseEntity.getBody() == null) {
                    logger.warn("{}", responseEntity);
                    throw new NullPointerException("Indexer Start Failed!");
                }
                indexingJobId = (String) responseEntity.getBody().get("id");
                indexingStatus.setScheme(launcher.getScheme());
                indexingStatus.setHost(launcher.getHost());
                indexingStatus.setPort(launcher.getPort());
            } else {
                body.put("username", collection.getEsUser());
                body.put("password", collection.getEsPassword());
                // 서버 쓰래드 기반으로 색인 실행.
                Job job = indexerJobManager.start(IndexerConfig.ACTION.FULL_INDEX.name(), body);
                indexingJobId = job.getId().toString();
            }

            logger.info("Job ID: {}", indexingJobId);
            indexingStatus.setClusterId(clusterId);
            indexingStatus.setIndex(index.getIndex());
            indexingStatus.setStartTime(System.currentTimeMillis());
            indexingStatus.setIndexingJobId(indexingJobId);
            indexingStatus.setAutoRun(autoRun);
            indexingStatus.setCurrentStep(step);
            indexingStatus.setNextStep(nextStep);
            indexingStatus.setRetry(50);
            indexingStatus.setCollection(collection);
        } catch (Exception e) {
            logger.error("", e);
            // TODO history 남길지 여부...
            throw new IndexingJobFailureException(e);
        }
        return indexingStatus;
    }

    /**
     * 색인 샤드의 TAG 제약을 풀고 전체 클러스터로 확장시킨다.
     * */
    public IndexingStatus propagate(UUID clusterId, boolean autoRun, Collection collection, String target) throws IndexingJobFailureException, IOException {
        return propagate(clusterId, autoRun, collection, new ArrayDeque<>(), target);
    }
    public IndexingStatus propagate(UUID clusterId, boolean autoRun, Collection collection, Queue<IndexStep> nextStep, String target) throws IndexingJobFailureException, IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();
            Map<String, Object> tmp = new HashMap<>() ;
            for(String key : propagate.keySet()){
                tmp.put(key, propagate.get(key));
            }

            if (target == null) {
                BoolQueryBuilder boolQuery = new BoolQueryBuilder();
                boolQuery.must(new MatchQueryBuilder("jobType", IndexStep.FULL_INDEX.name()));
                boolQuery.must(new MatchQueryBuilder("status", "SUCCESS"));

                // 마지막 성공했던 인덱스를 적용 되어 있으면 바꾸기
                QueryBuilder queryBuilder = boolQuery.should(new MatchQueryBuilder("index", indexA.getIndex()))
                        .should(new MatchQueryBuilder("index", indexB.getIndex()))
                        .minimumShouldMatch(1);

                SearchRequest searchRequest = new SearchRequest()
                        .indices(indexHistory)
                        .source(new SearchSourceBuilder().query(queryBuilder).size(1).from(0).sort("endTime", SortOrder.DESC));
                SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length != 1) {
                    throw new IndexingJobFailureException("propagate >> Not Matched");
                }
                Map<String, Object> source = searchHits[0].getSourceAsMap();
//                String index = (String) source.get("index");
                target = (String) source.get("index");
            }
            
            //설정 무시 옵션 있을시 전파시 role 설정 제거
            if(collection.getIgnoreRoleYn() != null && collection.getIgnoreRoleYn().equals("Y")) {
                tmp.remove("index.routing.allocation.include.role");
                tmp.remove("index.routing.allocation.exclude.role");
            } else {
                tmp.replace("index.routing.allocation.include.role", "");
                tmp.replace("index.routing.allocation.exclude.role", "index");
            }

            if(collection.getRefresh_interval() != null && collection.getRefresh_interval() != 0){
                tmp.replace("refresh_interval", collection.getRefresh_interval() + "s");
            }

            if(collection.getReplicas() != null && collection.getReplicas() >= 0){
                tmp.replace("index.number_of_replicas",collection.getReplicas());
            }
            
//            logger.info("propagate 시 셋팅 : {}", tmp);

            client.indices().putSettings(new UpdateSettingsRequest().indices(target).settings(tmp), RequestOptions.DEFAULT);
            IndexingStatus indexingStatus = new IndexingStatus();
            indexingStatus.setClusterId(clusterId);
            indexingStatus.setIndex(target);
            indexingStatus.setStartTime(System.currentTimeMillis());
            indexingStatus.setAutoRun(autoRun);
            indexingStatus.setCurrentStep(IndexStep.PROPAGATE);
            if (nextStep == null) {
                indexingStatus.setNextStep(new ArrayDeque<>());
            } else {
                indexingStatus.setNextStep(nextStep);
            }
            indexingStatus.setRetry(50);
            indexingStatus.setCollection(collection);
            return indexingStatus;
        }
    }

    /**
     * 색인을 대표이름으로 alias 하고 노출한다.
     * */
    public void expose(UUID clusterId, Collection collection) throws IOException {
        expose(clusterId, collection, null);
    }
    public void expose(UUID clusterId, Collection collection, String target) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            String baseId = collection.getBaseId();
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

            Collection.Index addIndex;
            Collection.Index removeIndex;
            IndicesAliasesRequest request = new IndicesAliasesRequest();

            if (target != null) {
                if (indexA.getIndex().equals(target)) {
                    addIndex = indexA;
                    removeIndex = indexB;
                } else {
                    addIndex = indexB;
                    removeIndex = indexA;
                }
                try {
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(addIndex.getIndex()).alias(baseId));
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                            .index(removeIndex.getIndex()).alias(baseId));
                    // 교체
                    client.indices().updateAliases(request, RequestOptions.DEFAULT);
                } catch (Exception e) {
                    request = new IndicesAliasesRequest();
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(addIndex.getIndex()).alias(baseId));
                    // 교체
                    client.indices().updateAliases(request, RequestOptions.DEFAULT);
                }
            } else {
                if (indexA.getUuid() == null && indexB.getUuid() == null) {
                    logger.debug("empty index");
                    return;
                } else if (indexA.getUuid() == null && indexB.getUuid() != null) {
//                인덱스가 하나일 경우 고정
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(indexB.getIndex()).alias(baseId));
                } else if (indexA.getUuid() != null && indexB.getUuid() == null) {
//                인덱스가 하나일 경우 고정
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(indexA.getIndex()).alias(baseId));
                } else {
                    // index_history 조회하여 마지막 인덱스, 전파 완료한 인덱스 검색.
                    QueryBuilder queryBuilder = new BoolQueryBuilder()
                            .must(new MatchQueryBuilder("jobType", "PROPAGATE"))
                            .must(new MatchQueryBuilder("status", "SUCCESS"))
                            .should(new MatchQueryBuilder("index", indexA.getIndex()))
                            .should(new MatchQueryBuilder("index", indexB.getIndex()))
                            .minimumShouldMatch(1);
                    SearchRequest searchRequest = new SearchRequest()
                            .indices(indexHistory)
                            .source(new SearchSourceBuilder().query(queryBuilder)
                                    .size(1)
                                    .from(0)
                                    .sort("endTime", SortOrder.DESC)
                            );
                    SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                    SearchHit[] searchHits = searchResponse.getHits().getHits();

                    if (searchHits.length == 1) {
//                index_history 조회하여 대상 찾음.
                        Map<String, Object> source = searchHits[0].getSourceAsMap();
                        String targetIndex = (String) source.get("index");

                        if (indexA.getIndex().equals(targetIndex)) {
                            addIndex = indexA;
                            removeIndex = indexB;
                        } else {
                            addIndex = indexB;
                            removeIndex = indexA;
                        }

                        request.addAliasAction(new IndicesAliasesRequest.
                                AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                                .index(addIndex.getIndex()).alias(baseId));
                        request.addAliasAction(new IndicesAliasesRequest.
                                AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                                .index(removeIndex.getIndex()).alias(baseId));

                    } else {
                        // default
                        request.addAliasAction(new IndicesAliasesRequest.
                                AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                                .index(indexA.getIndex()).alias(baseId));
                    }
                }

                // 교체
                client.indices().updateAliases(request, RequestOptions.DEFAULT);
            }
        }
    }


    /**
     * 전파 중지
     * */
    public void stopPropagation(UUID clusterId, Collection collection) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();
//            1. 대상 인덱스 찾기.
            Collection.Index index = getTargetIndex(collection.getBaseId(), indexA, indexB);

//            2. 인덱스 설정 변경.
            index.setStatus("STOP");
//            editPreparations(client, index); // 이전버전
            editPreparations(client, collection, index);
            logger.info("stop propagation{}", index.getIndex());
        }
    }

    private Collection.Index getTargetIndex(String baseId, Collection.Index indexA, Collection.Index indexB) {
        Collection.Index index;
        // 인덱스에 대한 alias를 확인
        if (indexA.getAliases().size() == 0 && indexB.getAliases().size() == 0) {
            index = indexA;
        } else if (indexA.getAliases().get(baseId) != null) {
            index = indexB;
        } else if (indexB.getAliases().get(baseId) != null) {
            index = indexA;
        } else {
            index = indexA;
        }
        logger.debug("choice Index: {}", index.getIndex());
        return index;
    }

//    private void editPreparations(RestHighLevelClient client, Collection.Index index) throws IOException {
//        logger.debug("indexing settings >>> {}", indexing);
//        if (index.getUuid() == null) {
//            // 인덱스 존재하지 않기 때문에 생성해주기.
//            // 인덱스 템플릿이 존재하기 때문에 맵핑 설정 패쓰
//            boolean isAcknowledged = client.indices().create(new CreateIndexRequest(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
//            logger.debug("create settings : {} ", isAcknowledged);
//        } else {
//            // 기존 인덱스가 존재하기 때문에 셋팅 설정만 변경함.
//            // settings에 index.routing.allocation.include._exclude=search* 호출
//            // refresh interval: -1로 변경. 색인도중 검색노출하지 않음. 성능향상목적.
//            boolean isAcknowledged = client.indices().putSettings(new UpdateSettingsRequest().indices(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
//            logger.debug("edit settings : {} ", isAcknowledged);
//        }
//    }


    private void editPreparations(RestHighLevelClient client, Collection collection, Collection.Index index) throws IOException {
        // 인덱스 존재하지 않기 때문에 생성해주기.
        // 인덱스 템플릿이 존재하기 때문에 맵핑 설정 패쓰
        // 셋팅무시 설정이 있을시 indexing 맵에서 role 제거

//        logger.info("collection >>> {}", collection.toString());
        if (index.getUuid() == null) {
            boolean isAcknowledged;
            if(collection.getIgnoreRoleYn() != null && collection.getIgnoreRoleYn().equals("Y")){
                Map<String, Object> tmp = new HashMap<>() ;
                for(String key : indexing.keySet()){
                    tmp.put(key, indexing.get(key));
                }

                tmp.remove("index.routing.allocation.include.role");
                tmp.remove("index.routing.allocation.exclude.role");
                logger.info("ES 인덱스 없을 시, Createing indexing settings >>> {}", tmp);

//                client.indices().create(new CreateIndexRequest("test-role").settings(tmp), RequestOptions.DEFAULT).isAcknowledged();
                isAcknowledged = client.indices().create(new CreateIndexRequest(index.getIndex()).settings(tmp), RequestOptions.DEFAULT).isAcknowledged();
            }else{
                indexing.replace("index.routing.allocation.include.role", "index");
                indexing.replace("index.routing.allocation.exclude.role", "");
                logger.info("ES 인덱스 없을 시, indexing settings >>> {}", indexing);
                isAcknowledged = client.indices().create(new CreateIndexRequest(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
            }
            logger.debug("create settings : {} ", isAcknowledged);
        } else {
            // 기존 인덱스가 존재하기 때문에 셋팅 설정만 변경함.
            // settings에 index.routing.allocation.include._exclude=search* 호출
            // refresh interval: -1로 변경. 색인도중 검색노출하지 않음. 성능향상목적.
            // 셋팅무시 설정이 있을시 indexing 맵에서 role 제거

            boolean isAcknowledged;
            if(collection.getIgnoreRoleYn() != null && collection.getIgnoreRoleYn().equals("Y")){
                Map<String, Object> tmp = new HashMap<>() ;
                for(String key : indexing.keySet()){
                    tmp.put(key, indexing.get(key));
                }
                tmp.remove("index.routing.allocation.include.role");
                tmp.remove("index.routing.allocation.exclude.role");
                logger.info("ES 인덱스 존재 시, Updated indexing settings >>> {}", tmp);

                isAcknowledged = client.indices().putSettings(new UpdateSettingsRequest().indices(index.getIndex()).settings(tmp), RequestOptions.DEFAULT).isAcknowledged();
            }else{
                indexing.replace("index.routing.allocation.include.role", "index");
                indexing.replace("index.routing.allocation.exclude.role", "");
                logger.info("ES 인덱스 존재 시, indexing settings >>> {}", indexing);
                isAcknowledged = client.indices().putSettings(new UpdateSettingsRequest().indices(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
            }
            logger.debug("edit settings : {} ", isAcknowledged);
        }
    }

    public Map<String, Object> convertRequestParams(String yamlStr) throws IndexingJobFailureException {
        Map<String, Object> convert = new HashMap<>(params);
//        default param mixed
//        convert.putAll(params);
        try {
            Map<String, Object> tmp = new Yaml().load(yamlStr);
            if (tmp != null) {
                convert.putAll(tmp);
            }
        } catch (ClassCastException | NullPointerException e) {
            throw new IndexingJobFailureException("invalid yaml");
        }
        return convert;
    }

    public void stopIndexing(String scheme, String host, int port, String jobId) {
        try {
            indexerJobManager.stop(UUID.fromString(jobId));
        } catch (Exception ignore) {  }
        try {
            restTemplate.exchange(new URI(String.format("%s://%s:%d/async/stop?id=%s", scheme, host, port, jobId)),
                    HttpMethod.PUT,
                    new HttpEntity(new HashMap<>()),
                    String.class
            );
        } catch (Exception ignore) { }
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public Map<String, Object> getIndexing() {
        return indexing;
    }

    public void setIndexing(Map<String, Object> indexing) {
        this.indexing = indexing;
    }

    public Map<String, Object> getPropagate() {
        return propagate;
    }

    public void setPropagate(Map<String, Object> propagate) {
        this.propagate = propagate;
    }

    public void setRefreshInterval(String refresh_interval) {
        this.propagate.put("refresh_interval", refresh_interval);
    }

    public String getRefreshInterval() {
        return (String) this.propagate.get("refresh_interval");
    }

    public Map<String,Object> getPropagateSettings() {
        return this.propagate;
    }

    public Map<String,Object> getIndexingSettings() {
        return this.indexing;
    }

    public void setPropagateSettings(Map<String, Object> settings) {
        this.propagate = settings;
    }

    public void setIndexingSettings(Map<String, Object> settings) {
        this.indexing = settings;
    }
}
