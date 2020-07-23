package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.entity.IndexStep;
import com.danawa.fastcatx.server.entity.IndexingStatus;
import com.danawa.fastcatx.server.excpetions.IndexingJobFailureException;
import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
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
@ConfigurationProperties(prefix = "fastcatx.collection")
public class IndexingJobService {
    private static Logger logger = LoggerFactory.getLogger(IndexingJobService.class);
    private final ElasticsearchFactory elasticsearchFactory;
    private RestTemplate restTemplate;

    private final String indexHistory = ".fastcatx_index_history";

    private final String jdbcSystemIndex;

    private Map<String, Object> params;
    private Map<String, Object> indexing;
    private Map<String, Object> propagate;

    public IndexingJobService(ElasticsearchFactory elasticsearchFactory,
                              @Value("${fastcatx.jdbc.setting}")String jdbcSystemIndex) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.jdbcSystemIndex = jdbcSystemIndex;

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    /**
     * 소스를 읽어들여 ES 색인에 입력하는 작업.
     * indexer를 외부 프로세스로 실행한다.
     *
     * @return */
    public IndexingStatus indexing(UUID clusterId, Collection collection, boolean autoRun, IndexStep step) throws IndexingJobFailureException {
        return indexing(clusterId, collection, autoRun, step, new ArrayDeque<>());
    }
    public IndexingStatus indexing(UUID clusterId, Collection collection, boolean autoRun, IndexStep step, Queue<IndexStep> nextStep) throws IndexingJobFailureException {
        IndexingStatus indexingStatus = new IndexingStatus();
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

//            1. 대상 인덱스 찾기.
            Collection.Index index = getTargetIndex(collection.getBaseId(), indexA, indexB);

//            2. 인덱스 설정 변경.
            editPreparations(client, index);

//            3. 런처 파라미터 변환작업
            Collection.Launcher launcher = collection.getLauncher();
            String host = launcher.getHost();
            int port = launcher.getPort();
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
            body.put("_indexingSettings", indexing);

//            4. indexer 색인 전송
            ResponseEntity<Map> responseEntity = restTemplate.exchange(
                    new URI(String.format("http://%s:%d/async/start", host, port)),
                    HttpMethod.POST,
                    new HttpEntity(body),
                    Map.class
            );

            String id = (String) responseEntity.getBody().get("id");
            logger.info("Job ID: {}", id);
//            indexingStatus.setCollectionId(collection.getId());
            indexingStatus.setClusterId(clusterId);
            indexingStatus.setIndex(index.getIndex());
            indexingStatus.setHost(host);
            indexingStatus.setStartTime(System.currentTimeMillis());
            indexingStatus.setPort(port);
            indexingStatus.setIndexingJobId(id);
            indexingStatus.setAutoRun(autoRun);
            indexingStatus.setCurrentStep(step);
            indexingStatus.setNextStep(nextStep);
            indexingStatus.setRetry(50);
            indexingStatus.setCollection(collection);
        } catch (Exception e) {
            logger.error("", e);
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


            client.indices().putSettings(new UpdateSettingsRequest().indices(target).settings(propagate), RequestOptions.DEFAULT);
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
                } catch (Exception e) {
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(addIndex.getIndex()).alias(baseId));
                }
                // 교체
                client.indices().updateAliases(request, RequestOptions.DEFAULT);

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
            editPreparations(client, index);
            logger.info("stop propagation{}", index.getIndex());
        }
    }

    private Collection.Index getTargetIndex(String baseId, Collection.Index indexA, Collection.Index indexB) {
        Collection.Index index = null;
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

    private void editPreparations(RestHighLevelClient client, Collection.Index index) throws IOException {
        logger.debug("indexing settings >>> {}", indexing);
        if (index.getUuid() == null) {
            // 인덱스 존재하지 않기 때문에 생성해주기.
            // 인덱스 템플릿이 존재하기 때문에 맵핑 설정 패쓰
            boolean isAcknowledged = client.indices().create(new CreateIndexRequest(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
            logger.debug("create settings : {} ", isAcknowledged);
        } else {
            // 기존 인덱스가 존재하기 때문에 셋팅 설정만 변경함.
            // settings에 index.routing.allocation.include._exclude=search* 호출
            // refresh interval: -1로 변경. 색인도중 검색노출하지 않음. 성능향상목적.
            boolean isAcknowledged = client.indices().putSettings(new UpdateSettingsRequest().indices(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
            logger.debug("edit settings : {} ", isAcknowledged);
        }
    }
    private Map<String, Object> convertRequestParams(String yamlStr) throws IndexingJobFailureException {
        Map<String, Object> convert = new HashMap<>();
//        default param mixed
        convert.putAll(params);
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

    public void stopIndexing(String host, int port, String jobId) {
        try {
            restTemplate.exchange(new URI(String.format("http://%s:%d/async/stop?id=%s", host, port, jobId)),
                    HttpMethod.PUT,
                    new HttpEntity(new HashMap<>()),
                    String.class
            );
        } catch (Exception e) {
            logger.warn("", e);
        }
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
}
