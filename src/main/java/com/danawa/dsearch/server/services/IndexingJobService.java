package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.config.IndexerConfig;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.entity.IndexStep;
import com.danawa.dsearch.server.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.fastcatx.indexer.IndexJobManager;
import com.danawa.fastcatx.indexer.entity.Job;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.tasks.CancelTasksRequest;
import org.elasticsearch.client.tasks.CancelTasksResponse;
import org.elasticsearch.client.tasks.TaskId;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
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
            logger.info("clusterId: {}, baseId: {}, 대상 인덱스 찾기", clusterId, collection.getBaseId());
//            Collection.Index index = getTargetIndex(collection.getBaseId(), indexA, indexB);
            Collection.Index index = getTargetIndex(client, collection.getBaseId(), indexA, indexB);

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

            
            // null 대비 에러처리
            if(collection.getEsHost() != null && !collection.getEsHost().equals("")){
                body.put("host", collection.getEsHost());    
            }

            int esPort = 9200;
            if(collection.getEsPort() != null && !collection.getEsPort().equals("")){
                try{
                    esPort = Integer.parseInt(collection.getEsPort());
                }catch (NumberFormatException e){
                    logger.info("{}", e);
                }
                body.put("port", esPort);
            }
            
            body.put("scheme", collection.getEsScheme());
            body.put("esUsername", collection.getEsUser());
            body.put("esPassword", collection.getEsPassword());

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

            Map<String, Object> newPropagateOptions = new HashMap<>() ;
            for(String key : propagate.keySet()){
                newPropagateOptions.put(key, propagate.get(key));
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
            
            // 설정 무시 옵션 있을시 전파시 role 설정 제거
            if(collection.getIgnoreRoleYn() != null && collection.getIgnoreRoleYn().equals("Y")) {
                newPropagateOptions.remove("index.routing.allocation.include.role");
                newPropagateOptions.remove("index.routing.allocation.exclude.role");
            } else {
                newPropagateOptions.replace("index.routing.allocation.include.role", "");
                newPropagateOptions.replace("index.routing.allocation.exclude.role", "index");
            }

            // refresh_interval : -1, 1s, 2s, ...
            if(collection.getRefresh_interval() != null){
                // 0일때는 1로 셋팅.
                int refresh_interval = collection.getRefresh_interval() == 0 ? 1 : collection.getRefresh_interval();

                if(refresh_interval >= 0){
                    newPropagateOptions.replace("refresh_interval", collection.getRefresh_interval() + "s");
                } else {
                    // -2 이하로 내려갈 때, -1로 고정.
                    refresh_interval = -1;
                    newPropagateOptions.replace("refresh_interval", refresh_interval + "");
                }
            }

            if(collection.getReplicas() == null){
                newPropagateOptions.replace("index.number_of_replicas", 1);
            } else if(collection.getReplicas() == 0){
                newPropagateOptions.replace("index.number_of_replicas", 0);
            } else {
                newPropagateOptions.replace("index.number_of_replicas", collection.getReplicas());
            }
            
            logger.info("propagate 셋팅 : {}", newPropagateOptions);
            client.indices().putSettings(new UpdateSettingsRequest().indices(target).settings(newPropagateOptions), RequestOptions.DEFAULT);
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

    public IndexingStatus reindex(UUID clusterId, boolean autoRun, Collection collection) throws IndexingJobFailureException, IOException {
        return reindex(clusterId, autoRun, collection, new ArrayDeque<>());
    }
    public IndexingStatus reindex(UUID clusterId, boolean autoRun, Collection collection, Queue<IndexStep> nextStep) throws IndexingJobFailureException, IOException {
        IndexingStatus indexingStatus = new IndexingStatus();
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

            // 대상 인덱스 찾기. source, dest
            logger.info("clusterId: {}, baseId: {}, 대상 인덱스 찾기", clusterId, collection.getBaseId());
            Collection.Index destIndex = getTargetIndex(client, collection.getBaseId(), indexA, indexB);
            Collection.Index sourceIndex = null;
            if (indexA.getIndex().equals(destIndex.getIndex())) {
                sourceIndex = indexB;
            } else {
                sourceIndex = indexA;
            }

            // 인덱스 설정 변경.
            logger.info("{} 인덱스 설정 변경", destIndex);
            editPreparations(client, collection, destIndex);

            // 타겟 인덱스 삭제
            boolean deleteConfirm = deleteIndex(clusterId, destIndex.getIndex());

            // 타겟 인덱스 삭제가 정상
            if(deleteConfirm) {
                // 인덱스 설정
                Map<String, Object> indexingSettings = new HashMap<>();
                for (String key : indexing.keySet()) {
                    indexingSettings.put(key, indexing.get(key));
                }
                if (collection.getIgnoreRoleYn() != null && collection.getIgnoreRoleYn().equals("Y")) {
                    indexingSettings.remove("index.routing.allocation.include.role");
                    indexingSettings.remove("index.routing.allocation.exclude.role");
                } else {
                    indexingSettings.replace("index.routing.allocation.include.role", "");
                    indexingSettings.replace("index.routing.allocation.exclude.role", "index");
                }
                logger.debug("indexingSettings:{}", indexingSettings);

                // 인덱스 생성
                boolean createConfirm = createIndex(clusterId, destIndex.getIndex(), indexingSettings);

                // 인덱스 생성이 정상
                if(createConfirm) {
                    // 런처 파라미터 변환작업
                    logger.info("{} 런처 파라미터 변환 작업", destIndex);
                    Collection.Launcher launcher = collection.getLauncher();
                    Map<String, Object> body = convertRequestParams(launcher.getYaml());

                    // reindex 설정값 default
                    // 배치 사이즈 default : 1000
                    int reindexBatchSize = 1000;
                    // sliced-scroll 슬라이스를 사용 (병렬) default : 1
                    int reindexSlices = 1;
                    if(body.get("reindexBatchSize") != null) {
                        try{
                            reindexBatchSize = Integer.parseInt(body.get("reindexBatchSize").toString());
                        }catch (NumberFormatException e){
                            logger.info("{}", e);
                        }
                    }
                    if(body.get("reindexSlices") != null) {
                        try{
                            reindexSlices = Integer.parseInt(body.get("reindexSlices").toString());
                        }catch (NumberFormatException e){
                            logger.info("{}", e);
                        }
                    }
                    logger.debug("reindexBatchSize:{}, reindexSlices:{}", reindexBatchSize, reindexSlices);

                    // reindex request 설정 세팅
                    ReindexRequest reindexRequest = new ReindexRequest();
                    reindexRequest.setSourceIndices(sourceIndex.getIndex());
                    reindexRequest.setDestIndex(destIndex.getIndex());
                    reindexRequest.setRefresh(true);
                    reindexRequest.setSourceBatchSize(reindexBatchSize);
                    reindexRequest.setSlices(reindexSlices);
                    // reindex 호출
                    TaskSubmissionResponse reindexSubmission = client.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
                    // 작업 번호
                    String taskId = reindexSubmission.getTask();
                    logger.info("taskId : {} - source : {} -> dest : {}", taskId, sourceIndex.getIndex(), destIndex.getIndex());

                    indexingStatus.setClusterId(clusterId);
                    indexingStatus.setTaskId(taskId);
                    indexingStatus.setIndex(destIndex.getIndex());
                    indexingStatus.setStartTime(System.currentTimeMillis());
                    indexingStatus.setAutoRun(autoRun);
                    indexingStatus.setCurrentStep(IndexStep.REINDEX);
                    if (nextStep == null) {
                        indexingStatus.setNextStep(new ArrayDeque<>());
                    } else {
                        indexingStatus.setNextStep(nextStep);
                    }
                    indexingStatus.setRetry(50);
                    indexingStatus.setCollection(collection);
                }
            }
        }
        return indexingStatus;
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
//            Collection.Index index = getTargetIndex(client, collection.getBaseId(), indexA, indexB);

//            2. 인덱스 설정 변경.
            index.setStatus("STOP");
//            editPreparations(client, index); // 이전버전
            editPreparations(client, collection, index);
            logger.info("stop propagation : {}", index.getIndex());
        }
    }

    public void stopReindexing(UUID clusterId, Collection collection, IndexingStatus indexingStatus) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

            Collection.Index index = getTargetIndex(collection.getBaseId(), indexA, indexB);
            // 취소 요청
            Request request = new Request("POST", String.format("/_tasks/%s/_cancel", indexingStatus.getTaskId()));
            request.addParameter("format", "json");
            Response response = client.getLowLevelClient().performRequest(request);
            String responseBodyString = EntityUtils.toString(response.getEntity());
            Map<String, Object> entityMap = new Gson().fromJson(responseBodyString, Map.class);
            //logger.info("entityMap : {}", entityMap);

            index.setStatus("STOP");
            logger.info("stop reindexing : {}", index.getIndex());
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


    private Collection.Index getTargetIndex(RestHighLevelClient client, String baseId, Collection.Index indexA, Collection.Index indexB) {
        Collection.Index index = indexA;
        // 인덱스에 대한 alias를 확인
        if (indexA.getAliases().size() == 0 && indexB.getAliases().size() == 0) {
            return index;
        }

        try{
            RestClient lowLevelClient = client.getLowLevelClient();
            Request request = new Request("GET", "_cat/aliases");
            request.addParameter("format", "json");
            Response response = lowLevelClient.performRequest(request);
            String entityString = EntityUtils.toString(response.getEntity());
            List<Map<String, Object>> entityMap = new Gson().fromJson(entityString, List.class);

            for(Map<String, Object> item : entityMap){
                if(item.get("alias").equals(baseId)){
                    String currentIndex = (String) item.get("index");
                    String suffix = currentIndex.substring(currentIndex.length()-2);

                    if(suffix.equals("-a")){
                        index = indexB;
                    }else if(suffix.equals("-b")){
                        index = indexA;
                    }else{
                        index = indexA;
                    }
                    break;
                }
            }

        }catch (IOException e){
            logger.error("{}", e);
        }

        return index;
    }

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
                logger.info("{}", index.getIndex());
//                client.indices().create(new CreateIndexRequest("test-role").settings(tmp), RequestOptions.DEFAULT).isAcknowledged();
                isAcknowledged = client.indices().create(new CreateIndexRequest(index.getIndex()).settings(tmp), RequestOptions.DEFAULT).isAcknowledged();
            }else{
                indexing.replace("index.routing.allocation.include.role", "index");
                indexing.replace("index.routing.allocation.exclude.role", "");
                logger.info("{}", index.getIndex());
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

    public void subStart(String scheme, String host, int port, String jobId, String groupSeq, boolean isExtIndexer) {
        if (isExtIndexer) {
            logger.info(">>>>> Ext call sub_start: id: {}, groupSeq: {}", jobId, groupSeq);
            try {
                restTemplate.exchange(new URI(String.format("%s://%s:%d/async/%s/sub_start?groupSeq=%s", scheme, host, port, jobId, groupSeq)),
                        HttpMethod.PUT,
                        new HttpEntity(new HashMap<>()),
                        String.class
                );
            } catch (Exception e) {
                logger.error("", e);
            }
        } else {
            try {
                logger.info(">>>>> Local call sub_start: id: {}, groupSeq: {}", jobId, groupSeq);
                Job job = indexerJobManager.status(UUID.fromString(jobId));
                job.getGroupSeq().add(Integer.parseInt(groupSeq));
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    public boolean deleteIndex(UUID clusterId, String index) throws IOException {
        boolean flag = false;
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            flag = deleteIndexResponse.isAcknowledged();
        }catch (Exception e){
            logger.error("", e);
        }
        return flag;
    }

    public boolean createIndex(UUID clusterId, String index, Map<String, ?> settings) throws IOException {
        boolean flag = false;
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            CreateIndexRequest request = new CreateIndexRequest(index);
            request.settings(settings);
            AcknowledgedResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            flag = createIndexResponse.isAcknowledged();
        } catch (Exception e) {
            logger.error("", e);
        }
        return flag;
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
