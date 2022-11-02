package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.notice.AlertService;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
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
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

@Service
@ConfigurationProperties(prefix = "dsearch.collection")
public class IndexingJobService {
    /**
     * 인덱서를 통해 색인 시작을 하기 전 타겟 인덱스에 대한 전처리/후처리를 담당합니다.
     */
    private static final Logger logger = LoggerFactory.getLogger(IndexingJobService.class);
    private final ElasticsearchFactory elasticsearchFactory;

    private final String indexHistory = ".dsearch_index_history";
    private final IndexerClient indexerClient;
    private final String jdbcSystemIndex;

    private AlertService alertService;
    private HistoryService indexHistoryService;
    private Map<String, Object> params;
    private Map<String, Object> indexing;

    public IndexingJobService(ElasticsearchFactory elasticsearchFactory,
                              @Value("${dsearch.jdbc.setting}") String jdbcSystemIndex,
                              HistoryService indexHistoryService,
                              AlertService alertService,
                              IndexerClient indexerClient) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.jdbcSystemIndex = jdbcSystemIndex;
        this.indexerClient = indexerClient;
        this.indexHistoryService = indexHistoryService;
        this.alertService  = alertService;
    }

    /**
     * 소스를 읽어들여 ES 색인에 입력하는 작업.
     * indexer를 외부 프로세스로 실행한다.
     *
     * @return IndexingStatus
     */
    public IndexingStatus indexing(UUID clusterId, Collection collection, boolean autoRun, IndexActionStep step) throws IndexingJobFailureException {
        return indexing(clusterId, collection, autoRun, step, new ArrayDeque<>());
    }

    public IndexingStatus indexing(UUID clusterId, Collection collection, boolean autoRun, IndexActionStep step, Queue<IndexActionStep> nextStep)
            throws IndexingJobFailureException {

        IndexingStatus indexingStatus = new IndexingStatus();
        Collection.Index index = null;

        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Collection.Index indexA = collection.getIndexA();
            Collection.Index indexB = collection.getIndexB();

            // 1. 대상 인덱스 찾기.
            index = getTargetIndex(client, collection.getBaseId(), indexA, indexB);

            // 2. 인덱스 설정 변경.
            editPreparations(client, index);

            // 3. 런처 파라미터 변환작업
            Collection.Launcher launcher = collection.getLauncher();
            Map<String, Object> body = convertRequestParams(launcher.getYaml());
            String jdbcId = collection.getJdbcId();
            if (jdbcId != null && !jdbcId.equals("null")) {
                if(!"".equals(jdbcId)){
                    GetResponse getResponse = client.get(new GetRequest().index(jdbcSystemIndex).id(collection.getJdbcId()), RequestOptions.DEFAULT);
                    Map<String, Object> jdbcSource = getResponse.getSourceAsMap();
                    jdbcSource.put("driverClassName", jdbcSource.get("driver"));
                    jdbcSource.put("url", jdbcSource.get("url"));
                    jdbcSource.put("user", jdbcSource.get("user"));
                    jdbcSource.put("password", jdbcSource.get("password"));
                    body.put("_jdbc", jdbcSource);
                }
            }
            body.put("index", index.getIndex());
            body.put("_indexingSettings", indexing);
            // null 대비 에러처리
            if (collection.getEsHost() != null && !collection.getEsHost().equals("")) {
                body.put("host", collection.getEsHost());
            }

            int esPort = 9200;
            if (collection.getEsPort() != null && !collection.getEsPort().equals("")) {
                try {
                    esPort = Integer.parseInt(collection.getEsPort());
                } catch (NumberFormatException e) {
                    logger.info("", e);
                }
                body.put("port", esPort);
            }

            body.put("scheme", collection.getEsScheme());
            body.put("esUsername", collection.getEsUser());
            body.put("esPassword", collection.getEsPassword());

            if (launcher.getScheme() == null || "".equals(launcher.getScheme())) {
                launcher.setScheme("http");
            }

            // 4. indexer 색인 전송
            String indexingJobId = indexerClient.startJob(body, collection);

            // 외부 인덱서를 사용할 경우 추가 셋팅
            if (collection.isExtIndexer()) {
                indexingStatus.setScheme(launcher.getScheme());
                indexingStatus.setHost(launcher.getHost());
                indexingStatus.setPort(launcher.getPort());
            }

            indexingStatus.setClusterId(clusterId);
            indexingStatus.setIndex(index.getIndex());
            indexingStatus.setStartTime(System.currentTimeMillis());
            indexingStatus.setIndexingJobId(indexingJobId);
            indexingStatus.setAutoRun(autoRun);
            indexingStatus.setCurrentStep(step);
            indexingStatus.setNextStep(nextStep);
            indexingStatus.setRetry(50);
            indexingStatus.setCollection(collection);

            logger.info("인덱싱 시작, clusterId={}, collection={}, index={}", clusterId, collection.getBaseId(), index);
        } catch (Exception e) {
            logger.error("{} 컬렉션 색인 실패 ==> {}", collection.getBaseId(), e);
            long errTime = System.currentTimeMillis();
            indexingStatus.setStatus("ERROR");
            indexingStatus.setAutoRun(collection.isAutoRun());
            indexingStatus.setStartTime(errTime);
            indexingStatus.setEndTime(errTime);
            indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX);
            indexingStatus.setIndex(index.getIndex());
            alertService.send(String.format("%s 컬렉션의 색인이 실패하였습니다.\n%s", collection.getBaseId(), e.getMessage()));
            indexHistoryService.create(indexingStatus, "0", "ERROR", "0");
            throw new IndexingJobFailureException(e);
        }

        return indexingStatus;
    }

    public void changeRefreshInterval(UUID clusterId, Collection collection, String target) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Map<String, Object> settings = new HashMap<>();
            // refresh_interval : -1, 1s, 2s, ...
            if (collection.getRefresh_interval() != null) {
                // 0일때는 1로 셋팅.
                int refresh_interval = collection.getRefresh_interval() == 0 ? 1 : collection.getRefresh_interval();

                if (refresh_interval >= 0) {
                    settings.put("refresh_interval", collection.getRefresh_interval() + "s");
                } else {
                    // -2 이하로 내려갈 때, -1로 고정.
                    refresh_interval = -1;
                    settings.put("refresh_interval", refresh_interval + "");
                }
            }

            client.indices().putSettings(new UpdateSettingsRequest().indices(target).settings(settings), RequestOptions.DEFAULT);
        }
    }

    /**
     * 색인을 대표이름으로 alias 하고 노출한다.
     */
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
                    // 인덱스가 하나일 경우 고정
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(indexB.getIndex()).alias(baseId));
                    target = indexB.getIndex();

                } else if (indexA.getUuid() != null && indexB.getUuid() == null) {
                    // 인덱스가 하나일 경우 고정
                    request.addAliasAction(new IndicesAliasesRequest.
                            AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                            .index(indexA.getIndex()).alias(baseId));
                    target = indexA.getIndex();
                } else {
                    // index_history 조회하여 마지막 인덱스, 색인 완료한 인덱스 검색.
                    QueryBuilder queryBuilder = new BoolQueryBuilder()
                            .must(new MatchQueryBuilder("jobType", "FULL_INDEX"))
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
                        // index_history 조회하여 대상 찾음.
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

                        target = addIndex.getIndex();
                    } else {
                        // default
                        request.addAliasAction(new IndicesAliasesRequest.
                                AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                                .index(indexA.getIndex()).alias(baseId));

                        target = indexA.getIndex();
                    }
                }

                // 교체
                client.indices().updateAliases(request, RequestOptions.DEFAULT);
            }

            changeRefreshInterval(clusterId, collection, target);
        }
    }

    private Collection.Index getTargetIndex(RestHighLevelClient client, String baseId, Collection.Index indexA, Collection.Index indexB) {
        Collection.Index index = indexA;
        // 인덱스에 대한 alias를 확인
        if (indexA.getAliases().size() == 0 && indexB.getAliases().size() == 0) {
            return index;
        }

        try {
            RestClient lowLevelClient = client.getLowLevelClient();
            Request request = new Request("GET", "_cat/aliases");
            request.addParameter("format", "json");
            Response response = lowLevelClient.performRequest(request);
            String entityString = EntityUtils.toString(response.getEntity());
            List<Map<String, Object>> entityMap = new Gson().fromJson(entityString, List.class);

            for (Map<String, Object> item : entityMap) {
                if (item.get("alias").equals(baseId)) {
                    String currentIndex = (String) item.get("index");
                    String suffix = currentIndex.substring(currentIndex.length() - 2);

                    if (suffix.equals("-a")) {
                        index = indexB;
                    } else if (suffix.equals("-b")) {
                        index = indexA;
                    } else {
                        index = indexA;
                    }
                    break;
                }
            }

        } catch (IOException e) {
            logger.error("", e);
        }

        return index;
    }

    private void editPreparations(RestHighLevelClient client, Collection.Index index) throws IOException {
        // 인덱스 존재하지 않기 때문에 생성해주기.
        // 인덱스 템플릿이 존재하기 때문에 맵핑 설정 패쓰
        if (index.getUuid() == null) {
            boolean isAcknowledged;
            logger.info("ES 인덱스 없을 시, indexing settings >>> {}", indexing);
            isAcknowledged = client.indices().create(new CreateIndexRequest(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
            logger.debug("create settings : {} ", isAcknowledged);
        } else {
            // 기존 인덱스가 존재하기 때문에 셋팅 설정만 변경함.
            // refresh interval: -1로 변경. 색인도중 검색노출하지 않음. 성능 향상 목적.
            // 셋팅무시 설정이 있을시 indexing 맵에서 role 제거
            logger.info("ES 인덱스 존재 시, indexing settings >>> {}", indexing);
            boolean isAcknowledged = client.indices().putSettings(new UpdateSettingsRequest().indices(index.getIndex()).settings(indexing), RequestOptions.DEFAULT).isAcknowledged();
            logger.debug("edit settings : {} ", isAcknowledged);
        }
    }

    public Map<String, Object> convertRequestParams(String yamlStr) throws IndexingJobFailureException {
        Map<String, Object> convert = new HashMap<>(params);
        logger.debug("{} {}", convert, yamlStr);
        try {
            Map<String, Object> tmp = new Yaml().load(yamlStr);
            if (tmp != null) {
                convert.putAll(tmp);
            }
        } catch (ClassCastException | NullPointerException e) {
            logger.error("{} {}", e.getMessage(), yamlStr);
            throw new IndexingJobFailureException("invalid yaml");
        }
        logger.info("{}", convert);
        return convert;
    }

    public void stopIndexing(IndexingStatus status) {
        try {
            indexerClient.stopJob(status);
        } catch (Exception ignore) {
        }
    }

    public void subStart(IndexingStatus status, Collection collection, String groupSeq) {
        try {
            indexerClient.subStart(status, collection, groupSeq);
        } catch (URISyntaxException ignore) {

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

    public Map<String, Object> getIndexingSettings() {
        return this.indexing;
    }

    public void setIndexingSettings(Map<String, Object> settings) {
        this.indexing = settings;
    }
}
