package com.danawa.dsearch.server.collections.service.indexing;

import com.danawa.dsearch.server.collections.service.indexer.IndexerClient;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexActionStep;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.notice.AlertService;
import com.google.gson.Gson;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
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
    private final ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryWrapper;

    private final String indexHistory = ".dsearch_index_history";
    private final IndexerClient indexerClient;
    private String jdbcSystemIndex = ".dsearch_jdbc";

    private AlertService alertService;
    private HistoryService indexHistoryService;
    private Map<String, Object> params = new HashMap<>();
    private Map<String, Object> indexing = new HashMap<>();

    public IndexingJobService(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryWrapper,
                              @Value("${dsearch.jdbc.setting}") String jdbcSystemIndex,
                              HistoryService indexHistoryService,
                              AlertService alertService,
                              IndexerClient indexerClient) {
        this.elasticsearchFactoryWrapper = elasticsearchFactoryWrapper;
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
    public IndexingStatus indexing(UUID clusterId, Collection collection, Queue<IndexActionStep> actionSteps) throws IndexingJobFailureException {
        IndexingStatus status = null;
        Collection.Index targetIndex = null;
        IndexActionStep currentStep = actionSteps.poll();

        try{
            // 1. 대상 인덱스 찾기.
            targetIndex = getTargetIndex(clusterId, collection);

            // 2. 인덱스 설정 변경.
            configureIndexSettings(clusterId, targetIndex);

            // 3. 런처 파라미터 변환 작업
            Map<String, Object> requestBody = makeRequestBodyToIndexer(clusterId, collection, targetIndex);

            // 4. indexer 색인 전송
            String indexingJobId = requestToIndexer(requestBody, collection);

            // 5. Indexing Status 셋팅하기
            status = makeIndexingStatus(clusterId, collection, targetIndex, indexingJobId, currentStep, actionSteps);

            logger.info("인덱싱 시작, clusterId={}, collection={}, index={}", clusterId, collection.getBaseId(), status.getIndex());
        }catch (IndexingJobFailureException e) {
            sendIndexingError(collection, targetIndex, e.getMessage());
            throw new IndexingJobFailureException(e.getMessage());
        }

        return status;
    }

    private Collection.Index getTargetIndex(UUID clusterId, Collection collection) throws IndexingJobFailureException {
        // 1. alias 가져오기
        String aliasString = getAliasString(clusterId);
        // 2. alias Json 변환
        // {
        //     "alias": "...",
        //     "index": "..."
        // }
        List<Map<String, Object>> aliasMap = new Gson().fromJson(aliasString, List.class);
        // 3. 현재 컬렉션의 alias가 있는지 찾기
        String collectionName = collection.getBaseId();
        if (isExistCollectionNameInAliases(aliasMap, collectionName)){
            // 3-1. 있다면 파싱해서 주기 
            return parseTargetIndex(aliasMap, collection);
        }else{
            // 3-2. 없다면 디폴트(IndexA)로 주기
            return getDefaultIndex(collection);
        }
    }
    private boolean isExistCollectionNameInAliases(List<Map<String, Object>> aliasMap, String baseId){
        for (Map<String, Object> item : aliasMap) {
            if (item.get("alias").equals(baseId)) {
                return true;
            }
        }

        return false;
    }


    private Collection.Index getDefaultIndex(Collection collection){
        if (collection.getIndexA() != null){
            return collection.getIndexA();
        }else if (collection.getIndexB() != null){
            return collection.getIndexB();
        }else{
            return collection.getIndexA();
        }
    }

    private String getAliasString(UUID clusterId) throws IndexingJobFailureException {
        try {
            return elasticsearchFactoryWrapper.getAliases(clusterId);
        }catch (IOException e){
            logger.error("", e);
            throw new IndexingJobFailureException(e.getMessage());
        }
    }

    private Collection.Index parseTargetIndex(List<Map<String, Object>> aliasMap, Collection collection) {
        String baseId = collection.getBaseId();
        Collection.Index targetIndex = collection.getIndexA();

        for (Map<String, Object> item : aliasMap) {
            if (item.get("alias").equals(baseId)) {

                String currentIndex = (String) item.get("index");
                String suffix = currentIndex.substring(currentIndex.length() - 2);

                if (suffix.equals("-a")) {
                    targetIndex = collection.getIndexB();
                } else if (suffix.equals("-b")) {
                    targetIndex = collection.getIndexA();
                }
                break;
            }
        }

        return targetIndex;
    }

    private void configureIndexSettings(UUID clusterId, Collection.Index index) {

        try {
            // 인덱스 존재하지 않기 때문에 생성해주기.
            // 인덱스 템플릿이 존재하기 때문에 맵핑 설정 패스
            if (index.getUuid() == null) {
                // refresh interval: -1로 셋팅
                logger.info("ES 인덱스 없을 시, indexing settings >>> {}", indexing);
                boolean isAcknowledged = elasticsearchFactoryWrapper.createIndexSettings(clusterId, index.getIndex(), indexing);
                logger.debug("create settings : {} ", isAcknowledged);
            } else {
                // refresh interval: -1로 변경. 색인도중 검색노출하지 않음. 성능 향상 목적.
                // 셋팅무시 설정이 있을시 indexing 맵에서 role 제거
                logger.info("ES 인덱스 존재 시, indexing settings >>> {}", indexing);
                boolean isAcknowledged = elasticsearchFactoryWrapper.updateIndexSettings(clusterId, index.getIndex(), indexing);
                logger.debug("edit settings : {} ", isAcknowledged);
            }
        }catch (IOException e){
            logger.error("{}", e);
        }
    }

    private Map<String, Object> makeRequestBodyToIndexer(UUID clusterId, Collection collection, Collection.Index index) throws IndexingJobFailureException {
        Collection.Launcher launcher = collection.getLauncher();
        if (launcher.getScheme() == null || "".equals(launcher.getScheme())) {
            launcher.setScheme("http");
        }

        Map<String, Object> body = convertRequestParams(launcher.getYaml());

        String jdbcId = collection.getJdbcId();
        Map<String, Object> jdbcContent = getJdbcContent(clusterId, jdbcId);
        body.put("_jdbc", jdbcContent);
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

        return body;
    }

    private Map<String, Object> getJdbcContent(UUID clusterId, String jdbcId){
        if (jdbcId != null && !jdbcId.equals("null") && "".equals(jdbcId)){
            return new HashMap<>();
        }

        try {
            Map<String, Object> jdbcSource = elasticsearchFactoryWrapper.getIndexDocument(clusterId, jdbcSystemIndex, jdbcId);
            jdbcSource.put("driverClassName", jdbcSource.get("driver"));
            jdbcSource.put("url", jdbcSource.get("url"));
            jdbcSource.put("user", jdbcSource.get("user"));
            jdbcSource.put("password", jdbcSource.get("password"));
            return jdbcSource;
        }catch (IOException e){
            return new HashMap<>();
        }
    }

    private String requestToIndexer(Map<String, Object> body, Collection collection) throws IndexingJobFailureException {
        try{
            String indexingJobId = indexerClient.startJob(body, collection);
            return indexingJobId;
        }catch (URISyntaxException e){
            logger.error("{}", e);
            throw new IndexingJobFailureException(e.getMessage());
        }
    }

    private IndexingStatus makeIndexingStatus(UUID clusterId,
                                              Collection collection,
                                              Collection.Index targetIndex,
                                              String indexingJobId,
                                              IndexActionStep currentStep,
                                              Queue<IndexActionStep> actionSteps){
        IndexingStatus indexingStatus = new IndexingStatus();
        Collection.Launcher launcher = collection.getLauncher();
        int retryCount = 50;

        // 외부 인덱서를 사용할 경우 추가 셋팅
        if (collection.isExtIndexer()) {
            indexingStatus.setScheme(launcher.getScheme());
            indexingStatus.setHost(launcher.getHost());
            indexingStatus.setPort(launcher.getPort());
        }

        indexingStatus.setClusterId(clusterId);
        indexingStatus.setIndex(targetIndex.getIndex());
        indexingStatus.setStartTime(System.currentTimeMillis());
        indexingStatus.setIndexingJobId(indexingJobId);
        indexingStatus.setAutoRun(collection.isAutoRun());
        indexingStatus.setCurrentStep(currentStep);
        indexingStatus.setNextStep(actionSteps);
        indexingStatus.setRetry(retryCount);
        indexingStatus.setCollection(collection);
        return indexingStatus;
    }

    private void sendIndexingError(Collection collection, Collection.Index targetIndex, String errMessage){
        long errTime = System.currentTimeMillis();
        IndexingStatus indexingStatus = new IndexingStatus();
        indexingStatus.setStatus("ERROR");
        indexingStatus.setAutoRun(collection.isAutoRun());
        indexingStatus.setStartTime(errTime);
        indexingStatus.setEndTime(errTime);
        indexingStatus.setCurrentStep(IndexActionStep.FULL_INDEX);

        if (targetIndex != null){
            indexingStatus.setIndex(targetIndex.getIndex());
        } else if(collection.getIndexA() != null){
            indexingStatus.setIndex(collection.getIndexA().getIndex());
        } else if(collection.getIndexB() != null){
            indexingStatus.setIndex(collection.getIndexB().getIndex());
        }

        alertService.send(String.format("%s 컬렉션의 색인이 실패하였습니다.\n%s", collection.getBaseId(), errMessage));
        indexHistoryService.create(indexingStatus, "0", "ERROR", "0");
    }

    public void changeRefreshInterval(UUID clusterId, Collection collection, String target) throws IOException {
        Map<String, Object> settings = new HashMap<>();
        // 인덱스 설정에 있는 refresh interval
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
        elasticsearchFactoryWrapper.updateIndexSettings(clusterId, target, settings);
    }

    /**
     * 색인을 대표이름으로 alias 하고 노출한다.
     */
    public void expose(UUID clusterId, Collection collection) throws IOException {
        expose(clusterId, collection, null);
    }

    public void expose(UUID clusterId, Collection collection, String targetIndex) throws IOException {
        if(!isCreatedIndexes(clusterId, collection)){
            logger.info("{} 컬렉션의 인덱스가 생성되지 않아 Expose 중지", collection.getBaseId());
            return;
        }

        if (targetIndex != null) {
            changeAliasWithTargetIndex(clusterId, collection, targetIndex);
        } else {
            targetIndex = changeAliasWithoutTargetIndex(clusterId, collection);
        }

        changeRefreshInterval(clusterId, collection, targetIndex);
    }
    private boolean isCreatedIndexes(UUID clusterId, Collection collection) throws IOException {
        boolean isExistsIndexA = elasticsearchFactoryWrapper.isExistIndex(clusterId, collection.getIndexA().getIndex());
        boolean isExistsIndexB = elasticsearchFactoryWrapper.isExistIndex(clusterId, collection.getIndexB().getIndex());

        if (isExistsIndexA == false && isExistsIndexB == false) {
            logger.debug("현재 인덱스가 생성되지 않음");
            return false;
        }

        return true;
    }

    private void changeAliasWithTargetIndex(UUID clusterId, Collection collection, String targetIndex) throws IOException{
        IndicesAliasesRequest request = new IndicesAliasesRequest();

        String baseId = collection.getBaseId();
        Collection.Index indexA = collection.getIndexA();
        Collection.Index indexB = collection.getIndexB();


        String nameOfindexA = indexA.getIndex() != null ? indexA.getIndex() : "" ;
        String nameOfindexB = indexB.getIndex() != null ? indexB.getIndex() : "" ;

        String toAddIndex = getIndexToAddAlias(nameOfindexA, nameOfindexB, targetIndex);
        String toRemoveIndex = getIndexToRemoveAlias(nameOfindexA, nameOfindexB, targetIndex);

        try {
            request.addAliasAction(new IndicesAliasesRequest.
                    AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                    .index(toAddIndex).alias(baseId));
            request.addAliasAction(new IndicesAliasesRequest.
                    AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                    .index(toRemoveIndex).alias(baseId));
            elasticsearchFactoryWrapper.updateAliases(clusterId, request);
        } catch (Exception e) {
            request = new IndicesAliasesRequest();
            request.addAliasAction(new IndicesAliasesRequest.
                    AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                    .index(toAddIndex).alias(baseId));
            // 교체
            elasticsearchFactoryWrapper.updateAliases(clusterId, request);
        }
    }

    private String getIndexToAddAlias(String indexA, String indexB, String targetIndex){
        if (indexA.equals(targetIndex)) {
            return indexA;
        } else {
            return indexB;
        }
    }

    private String getIndexToRemoveAlias(String indexA, String indexB, String targetIndex){
        if (indexA.equals(targetIndex)) {
            return indexB;
        } else {
            return indexA;
        }
    }

    private String changeAliasWithoutTargetIndex(UUID clusterId, Collection collection) throws IOException {
        IndicesAliasesRequest request = new IndicesAliasesRequest();

        String baseId = collection.getBaseId();
        Collection.Index indexA = collection.getIndexA();
        Collection.Index indexB = collection.getIndexB();

        String targetIndex;

        if (isExistsIfOnlyIndexB(collection)) {
            // 1. -b 인덱스만 있을 경우
            request.addAliasAction(new IndicesAliasesRequest.
                    AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                    .index(indexB.getIndex()).alias(baseId));
            targetIndex = indexB.getIndex();
        } else if (isExistsIfOnlyIndexA(collection)) {
            // 2. -a 인덱스만 있을 경우
            request.addAliasAction(new IndicesAliasesRequest.
                    AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                    .index(indexA.getIndex()).alias(baseId));
            targetIndex = indexA.getIndex();
        } else {
            // 3. -a, -b 인덱스가 있을 경우
            // 인덱스 히스토리 로그를 찾아 최근에 성공한 인덱스를 찾는다
            targetIndex = findRecentlyIndexingSuccessIndex(clusterId, indexA.getIndex(), indexB.getIndex());

            if ("".equals(targetIndex)) {
                // 3.1) -a, -b 인덱스 둘다 있지만 alias가 셋팅이 되지 않은 경우
                request.addAliasAction(new IndicesAliasesRequest.
                        AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(indexA.getIndex()).alias(baseId));

                targetIndex = indexA.getIndex();
            } else {
                // 3.2) -a, -b 인덱스 둘다 있지만 alias가 셋팅이 되어 있는 경우
                String addIndex = getIndexToAddAlias(indexA.getIndex(), indexB.getIndex(), targetIndex);
                String removeIndex = getIndexToRemoveAlias(indexA.getIndex(), indexB.getIndex(), targetIndex);

                request.addAliasAction(new IndicesAliasesRequest.
                        AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(addIndex).alias(baseId));
                request.addAliasAction(new IndicesAliasesRequest.
                        AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        .index(removeIndex).alias(baseId));
            }
        }

        // Alias 교체
        elasticsearchFactoryWrapper.updateAliases(clusterId, request);
        return targetIndex;
    }

    private boolean isExistsIfOnlyIndexA(Collection collection){
        Collection.Index indexA = collection.getIndexA();
        Collection.Index indexB = collection.getIndexB();
        return indexA.getUuid() != null && indexB.getUuid() == null;
    }

    private boolean isExistsIfOnlyIndexB(Collection collection){
        Collection.Index indexA = collection.getIndexA();
        Collection.Index indexB = collection.getIndexB();
        return indexA.getUuid() == null && indexB.getUuid() != null;
    }

    private String findRecentlyIndexingSuccessIndex(UUID clusterId, String indexA, String indexB) throws IOException {
        // index_history 조회하여 마지막 인덱스, 색인 완료한 인덱스 검색.
        QueryBuilder queryBuilder = new BoolQueryBuilder()
                .must(new MatchQueryBuilder("jobType", "FULL_INDEX"))
                .must(new MatchQueryBuilder("status", "SUCCESS"))
                .should(new MatchQueryBuilder("index", indexA))
                .should(new MatchQueryBuilder("index", indexB))
                .minimumShouldMatch(1);
        SearchRequest searchRequest = new SearchRequest()
                .indices(indexHistory)
                .source(new SearchSourceBuilder().query(queryBuilder)
                        .size(1)
                        .from(0)
                        .sort("endTime", SortOrder.DESC)
                );

        SearchHit[] searchHits = elasticsearchFactoryWrapper.search(clusterId, searchRequest);

        if(searchHits.length == 1){
            // index_history 조회하여 대상 찾음.
            Map<String, Object> source = searchHits[0].getSourceAsMap();
            String targetIndex = (String) source.get("index");
            return targetIndex;
        }else{
            return "";
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

    public void startGroupJob(IndexingStatus status, Collection collection, String groupSeq) {
        try {
            indexerClient.startGroupJob(status, collection, groupSeq);
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
