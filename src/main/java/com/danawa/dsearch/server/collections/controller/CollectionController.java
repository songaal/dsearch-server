package com.danawa.dsearch.server.collections.controller;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.collections.entity.IndexingAction;
import com.danawa.dsearch.server.collections.service.schedule.IndexingScheduler;
import com.danawa.dsearch.server.collections.service.indexing.IndexingJobManager;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexingInfo;
import com.danawa.dsearch.server.collections.service.indexing.IndexingService;
import com.danawa.dsearch.server.excpetions.DuplicatedUserException;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.excpetions.ParameterInvalidException;
import com.danawa.dsearch.server.utils.YamlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/collections")
public class CollectionController {
    private static Logger logger = LoggerFactory.getLogger(CollectionController.class);

    private final String indexSuffixA;
    private final String indexSuffixB;
    private final ClusterService clusterService;
    private final CollectionService collectionService;
    private final IndexingJobManager indexingJobManager;
    private final IndexingScheduler scheduleManager;
    private final IndexingService indexingService;

    public CollectionController(@Value("${dsearch.collection.index-suffix-a}") String indexSuffixA,
                                @Value("${dsearch.collection.index-suffix-b}") String indexSuffixB,
                                CollectionService collectionService,
                                ClusterService clusterService,
                                IndexingJobManager indexingJobManager,
                                IndexingScheduler scheduleManager,
                                IndexingService indexingService) {
        this.indexSuffixA = indexSuffixA;
        this.indexSuffixB = indexSuffixB;
        this.collectionService = collectionService;
        this.indexingJobManager = indexingJobManager;
        this.clusterService = clusterService;
        this.scheduleManager = scheduleManager;
        this.indexingService = indexingService;
    }

    /**
     * 컬렉션 추가 메서드
     * @param clusterId
     * @param collection
     * @return
     * @throws IOException
     * @throws DuplicatedUserException
     */
    @PostMapping
    public ResponseEntity<?> addCollection(@RequestHeader(value = "cluster-id") UUID clusterId,
                                           @RequestBody Collection collection) throws IOException, DuplicatedUserException {
        collectionService.create(clusterId, collection);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 컬렉션 삭제 메서드
     * @param clusterId
     * @param id
     * @return
     * @throws IOException
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteById(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @PathVariable String id) throws IOException {
        collectionService.deleteById(clusterId, id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    /**
     * 컬렉션 전체 리스트 가져오기
     * @param clusterId
     * @param action
     * @return
     * @throws IOException
     */
    @GetMapping
    public ResponseEntity<?> findAll(@RequestHeader(value = "cluster-id") UUID clusterId,
                                     @RequestParam(defaultValue = "collection") String action) throws IOException {
        Map<String, Object> response = new HashMap<>();
        if ("indexSuffix".equalsIgnoreCase(action)) {
            response.put("indexSuffixA", indexSuffixA);
            response.put("indexSuffixB", indexSuffixB);
        } else if ("collection".equalsIgnoreCase(action)) {
            response.put("list", collectionService.findAll(clusterId));
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * 특정 컬렉션만 가져오기
     * @param clusterId
     * @param id
     * @return
     * @throws IOException
     */
    @GetMapping("/{id}")
    public ResponseEntity<?> findById(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @PathVariable String id) throws IOException {
        return new ResponseEntity<>(collectionService.findById(clusterId, id), HttpStatus.OK);
    }

    /**
     * 특정 컬렉션 수정
     * @param clusterId
     * @param action
     * @param collectionId
     * @param collection
     * @return
     * @throws IOException
     */
    @PutMapping("/{collectionId}")
    public ResponseEntity<?> editCollection(@RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestParam String action,
                                            @PathVariable String collectionId,
                                            @RequestBody Collection collection) throws IOException {
        collection.setId(collectionId);
        logger.info("action: {}, collectionId: {}, baseId: {}", action, collectionId, collection.getBaseId());

        if ("source".equalsIgnoreCase(action)) {
            Map<String, Object> source = collectionService.updateSource(clusterId, collectionId, collection);

            boolean isScheduled = (boolean) source.get("scheduled");
            logger.info("isScheduled={} collection={}", isScheduled, collection);
            if(isScheduled){
                // collection에는 따로 셋팅해서 넘겨준다. -> sourceAsMap에는 있지만 Collection에는 false로 등록되어 있을 수 있기 때문에.
                scheduleManager.reload(clusterId, collection.getId()); // 스케줄 리셋
            }
        } else if ("schedule".equalsIgnoreCase(action)) {
            collectionService.updateSchedule(clusterId, collectionId, collection);
            scheduleManager.reload(clusterId, collection.getId()); // 스케줄이 업데이트 되었으므로 기존 데이터 삭제 후 재등록 필요

            logger.info("등록된 스케쥴 리스트");
            List<String> jobs = scheduleManager.getScheduleList();
            Collections.sort(jobs);
            for (String job: jobs){
                logger.info("{}", job);
            }
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }


    /**
     * 현재 색인 중인 특정 컬렉션 내역 가져오기
     * @param clusterId
     * @param id
     * @return
     */
    @GetMapping("/{id}/job")
    public ResponseEntity<?> getIndexingJobList(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @PathVariable String id) {
        return new ResponseEntity<>(indexingJobManager.getManageQueue(id), HttpStatus.OK);
    }

    /**
     * 현재 색인중인 전체 컬렉션 가져오기
     * @return
     */
    @GetMapping("/manageQueue")
    public ResponseEntity<?> getManageQueue(){
        Map<String, Object> response = new HashMap<>();
        List<IndexingInfo> statusList = indexingJobManager.getManageQueueList();
        response.put("message", "");
        response.put("data",  statusList);
        response.put("result", "success");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * 색인 중 혹은 색인이 끝난 전체 컬렉션 가져오기
     * @return
     */
    @GetMapping("/lookupQueue")
    public ResponseEntity<?> getLookupQueue(){
        Map<String, Object> response = new HashMap<>();
        List<IndexingInfo> statusList = indexingJobManager.getLookupQueueList();
        response.put("message", "");
        response.put("data",  statusList);
        response.put("result", "success");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @GetMapping("/scheduleQueue")
    public ResponseEntity<?> getScheduleQueue(){
        Map<String, Object> response = new HashMap<>();
        List<String> statusList = scheduleManager.getScheduleList();
        response.put("message", "");
        response.put("data",  statusList);
        response.put("result", "success");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/setTimeout")
    public ResponseEntity<?> setRefreshInterval(@RequestParam String timeout) {
        indexingJobManager.setTimeout(Long.parseLong(timeout));
        return new ResponseEntity<>(HttpStatus.OK);
    }


    /**
     * 색인 하기 - 색인, 교체, 그룹색인, 색인 중지
     * @param clusterId
     * @param clientIP
     * @param id
     * @param action
     * @return
     * @throws IndexingJobFailureException
     * @throws IOException
     */
    @PutMapping("/{id}/action")
    public ResponseEntity<?> indexing(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @RequestHeader(value = "client-ip", required = false) String clientIP,
                                      @PathVariable String id,
                                      @RequestParam String action) throws IndexingJobFailureException, IOException {
        IndexingAction actionType = getActionType(action);
        Collection collection = collectionService.findById(clusterId, id);
        logger.info("[action] clusterId={}, clientIP={}, collection={}, actionType={}",
                clusterId,
                clientIP,
                collection,
                actionType);
        Map<String, Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, "");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * 색인 하기 
     * @param host
     * @param port
     * @param collectionName
     * @param groupSeq
     * @param action
     * @return
     * @throws IndexingJobFailureException
     * @throws IOException
     * @throws ParameterInvalidException
     */
    @GetMapping("/idxp")
    public ResponseEntity<?> idxp(@RequestParam(name = "host") String host,
                                  @RequestParam(name = "port") int port,
                                  @RequestParam(name = "collectionName") String collectionName,
                                  @RequestParam(required = false) String groupSeq,
                                  @RequestParam(name = "action") String action) throws IndexingJobFailureException, IOException, ParameterInvalidException {
        validateParams(host, port, collectionName, action);
        Cluster cluster = null;
        Collection collection = null;

        try{
            cluster = clusterService.findByHostAndPort(host, port);
            collection = collectionService.findByName(cluster.getId(), collectionName);
        }catch (NoSuchElementException e){
            throw new IndexingJobFailureException(e.getMessage());
        }

        UUID clusterId = cluster.getId();
        IndexingAction actionType = getActionType(action);

        // IDXP에서 groupSeq가 넘어오면서 전체 색인을 시작한다.
        // 일반적으로 관리도구 설정과 동일할거같지만... IDXP 파라미터가 있을 경우 groupSeq 를 변경한다.
        if (isValidGroupSeq(actionType, groupSeq)) {
            changeGroupSeqWithinLauncher(collection, groupSeq);
        }
        logger.info("[idxp] clusterId={}, clientIP=from-remote-indexer, collection={}, actionType={}, groupSeq={}",
                clusterId,
                collection,
                actionType,
                groupSeq);
        Map<String,Object> response = indexingService.processIndexingJob(clusterId, collection, actionType, groupSeq);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    /**
     * 색인 내역 가져오기
     * @param host
     * @param port
     * @param collectionName
     * @return
     * @throws IOException
     * @throws IndexingJobFailureException
     * @throws ParameterInvalidException
     */
    @GetMapping("/idxp/status")
    public ResponseEntity<?> getIdxpStatus(@RequestParam(name = "host") String host,
                                           @RequestParam(name = "port") int port,
                                           @RequestParam(name = "collectionName") String collectionName) throws IOException, IndexingJobFailureException, ParameterInvalidException {

        logger.info("/idxp/status");
        validateParams(host, port, collectionName, "empty");
        Cluster cluster = null;
        Collection collection = null;

        try{
            cluster = clusterService.findByHostAndPort(host, port);
            collection = collectionService.findByName(cluster.getId(), collectionName);
        }catch (NoSuchElementException e){
            throw new IndexingJobFailureException(e.getMessage());
        }

        String collectionId = collection.getId();
        IndexingInfo indexingInfo = indexingJobManager.getCurrentIndexingStatus(collectionId);

        Map<String, Object> response = makeIndexingStatusResponse(indexingInfo);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private boolean isValidGroupSeq(IndexingAction actionType, String groupSeq){
        return (actionType == IndexingAction.ALL || actionType == IndexingAction.INDEXING) && groupSeq != null && !"".equals(groupSeq);
    }
    private void changeGroupSeqWithinLauncher(Collection collection, String groupSeq){
        try {
            Map<String ,Object> yamlToMap = YamlUtils.convertYamlToMap(collection.getLauncher().getYaml());
            if (yamlToMap.get("groupSeq") != null && !"".equals(yamlToMap.get("groupSeq"))) {
                yamlToMap.put("groupSeq", groupSeq);
                makePrettyLauncher(collection, yamlToMap);
            }
        } catch(Exception e) {
            logger.error("", e);
        }
    }

    private void makePrettyLauncher(Collection collection, Map<String, Object> yamlToMap){
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        collection.getLauncher().setYaml(new Yaml(options).dump(yamlToMap));
    }


    private Map<String, Object> makeIndexingStatusResponse(IndexingInfo indexingInfo){
        Map<String, Object> response = new HashMap<>();

        if(indexingInfo == IndexingInfo.Empty){
            Map<String, String> map = new HashMap<>();
            map.put("status", "NOT_STARTED");
            response.put("message", "Not Found Status (색인을 시작하지 않았습니다)");
            response.put("info", map);
            return response;
        }

        response.put("result", "success");
        response.put("message", "");
        response.put("info", indexingInfo);
        response.put("step",  indexingInfo.getCurrentStep());
        return response;
    }



    /**
     * 도우미 메서드 영역
     * */
    private IndexingAction getActionType(String action) throws IndexingJobFailureException{
        action = action.toLowerCase();
        switch (action) {
            case "all":
                return IndexingAction.ALL;
            case "indexing":
                return IndexingAction.INDEXING;
            case "expose":
                return IndexingAction.EXPOSE;
            case "stop_propagation":
                return IndexingAction.STOP_PROPAGATION;
            case "stop_indexing":
                return IndexingAction.STOP_INDEXING;
            case "sub_start":
                return IndexingAction.SUB_START;
            case "stop_reindexing":
                return IndexingAction.STOP_REINDEXING;
            default:
                throw new IndexingJobFailureException("Not Found action Type : " + action);
        }
    }

    private void validateParams(String host, int port, String collectionName, String action) throws ParameterInvalidException {
        // Host 에러 처리
        if(host == null || host.equals("")){
            throw new ParameterInvalidException("host is empty");
        }

        // Port 에러 처리
        if (port <= 0) {
            throw new ParameterInvalidException("port is less than 0");
        }

        // CollectionName 에러 처리
        if(collectionName == null || collectionName.equals("")){
            throw new ParameterInvalidException("collection name is null");
        }

        // action 에러 처리
        if (action == null || action.equals("")) {
            throw new ParameterInvalidException("action is null");
        }
    }
}