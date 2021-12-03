package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.Cluster;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.entity.IndexStep;
import com.danawa.dsearch.server.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.DuplicateException;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import com.danawa.dsearch.server.services.ClusterService;
import com.danawa.dsearch.server.services.CollectionService;
import com.danawa.dsearch.server.services.IndexingJobManager;
import com.danawa.dsearch.server.services.IndexingJobService;
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

    private static Object obj = new Object();
    private final String indexSuffixA;
    private final String indexSuffixB;
    private final ClusterService clusterService;
    private final CollectionService collectionService;
    private final IndexingJobService indexingJobService;
    private final IndexingJobManager indexingJobManager;

    public CollectionController(@Value("${dsearch.collection.index-suffix-a}") String indexSuffixA,
                                @Value("${dsearch.collection.index-suffix-b}") String indexSuffixB,
                                CollectionService collectionService, ClusterService clusterService,
                                IndexingJobService indexingJobService, IndexingJobManager indexingJobManager) {
        this.indexSuffixA = indexSuffixA;
        this.indexSuffixB = indexSuffixB;
        this.collectionService = collectionService;
        this.indexingJobService = indexingJobService;
        this.indexingJobManager = indexingJobManager;
        this.clusterService = clusterService;
    }

    @PostMapping
    public ResponseEntity<?> addCollection(@RequestHeader(value = "cluster-id") UUID clusterId,
                                           @RequestBody Collection collection) throws IOException, DuplicateException {
        collectionService.add(clusterId, collection);
        return new ResponseEntity<>(HttpStatus.OK);
    }

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

    @GetMapping("/{id}")
    public ResponseEntity<?> findById(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @PathVariable String id) throws IOException {
        return new ResponseEntity<>(collectionService.findById(clusterId, id), HttpStatus.OK);
    }

    @GetMapping("/{id}/job")
    public ResponseEntity<?> getJob(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @PathVariable String id) {
        return new ResponseEntity<>(indexingJobManager.findById(id), HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteById(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @PathVariable String id) throws IOException {
        collectionService.deleteById(clusterId, id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> editCollection(@RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestParam String action,
                                            @PathVariable String id,
                                            @RequestBody Collection collection) throws IOException {

        logger.info("action: {}, id: {}, collection: {}", action, id, collection.getBaseId());
        if ("source".equalsIgnoreCase(action)) {
            collectionService.editSource(clusterId, id, collection);
        } else if ("schedule".equalsIgnoreCase(action)) {
            collectionService.editSchedule(clusterId, id, collection);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}/action")
    public ResponseEntity<?> indexing(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @PathVariable String id,
                                      @RequestParam String action) throws IndexingJobFailureException, IOException {
        Map<String, Object> response = new HashMap<>();
        IndexingActionType actionType = getActionType(action);
        Collection collection = collectionService.findById(clusterId, id);

        registerIndexingJob(clusterId, id, collection, actionType, "", response);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/idxp")
    public ResponseEntity<?> idxp(@RequestParam String host,
                                      @RequestParam String port,
                                      @RequestParam String collectionName,
                                      @RequestParam(required = false) String groupSeq,
                                      @RequestParam String action) throws IndexingJobFailureException, IOException {
        Map<String, Object> response = new HashMap<>();

        handleError(host, port, collectionName, action, response);

        int parsePort = Integer.parseInt(port);

        List<Cluster> clusterList = clusterService.findByHostAndPort(host, parsePort);

        if(clusterList == null || clusterList.size() == 0){
            // 클러스터가 없을때
            response.put("message", "Not Found Cluster");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        // 호스트명과 포트가 같으니 같은 ES이다
        // 즉, 아무거나 가져와도 됨
        Cluster cluster = clusterList.get(0);
        Collection collection = collectionService.findByName(cluster.getId(), collectionName);

        if(collection == null){
            //컬렉션이 없을때
            response.put("message", "Not Found Collection Name");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        UUID clusterId = cluster.getId();
        String id = collection.getId();

        IndexingActionType actionType = getActionType(action);

//        IDXP에서 groupSeq가 넘어오면서 전체 색인을 시작한다.
//        일반적으로 관리도구 설정과 동일할거같지만... IDXP 파라미터가 있을 경우 groupSeq 를 변경한다.
//        TODO IDXP 개선때 변경예정

        if ((actionType == IndexingActionType.ALL || actionType == IndexingActionType.INDEXING) && groupSeq != null && !"".equals(groupSeq)) {
            try {
                Map<String ,Object> yamlToMap = indexingJobService.convertRequestParams(collection.getLauncher().getYaml());
                if (yamlToMap.get("groupSeq") != null && !"".equals(yamlToMap.get("groupSeq"))) {
                    yamlToMap.put("groupSeq", groupSeq);
                    DumperOptions options = new DumperOptions();
                    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
                    options.setPrettyFlow(true);
                    collection.getLauncher().setYaml(new Yaml(options).dump(yamlToMap));
                }
            } catch(Exception e) {
                logger.error("", e);
            }
        }

        // 인덱싱 도우미 메서드
        registerIndexingJob(clusterId, id, collection, actionType, groupSeq, response);

        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @GetMapping("/idxp/status")
    public ResponseEntity<?> getIdxpStatus(@RequestParam String host,
                                  @RequestParam String port,
                                  @RequestParam String collectionName) throws IOException {
        Map<String, Object> response = new HashMap<>();

        handleError(host, port, collectionName, "empty", response);
        int parsePort = Integer.parseInt(port);

        List<Cluster> clusterList = clusterService.findByHostAndPort(host, parsePort);
        if(clusterList == null || clusterList.size() == 0){
            response.put("message", "Not Found Cluster");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        Cluster cluster = clusterList.get(0);
        Collection collection = collectionService.findByName(cluster.getId(), collectionName);
        if(collection == null){
            response.put("message", "Not Found Collection Name");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }


        String id = collection.getId();
        IndexingStatus indexingStatus = indexingJobManager.getIndexingStatus(id);
        if(indexingStatus == null){
            Map<String, String> map = new HashMap<>();
            map.put("status", "NOT_STARTED");
            response.put("message", "Not Found Status (색인을 시작하지 않았습니다)");
            response.put("result", "success");
            response.put("info", map);
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        // KEY: collection id, value: Indexing status
//        Map<String, Object> server = (Map<String, Object>) indexingStatus.get(id);
//        response.put("info", server.get("server"));
        response.put("message", "");
        response.put("info",  indexingStatus);
        response.put("step",  indexingStatus.getCurrentStep());
        response.put("result", "success");

        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/setTimeout")
    public ResponseEntity<?> setRefreshInterval(@RequestParam String timeout) {
        indexingJobManager.setTimeout(Long.parseLong(timeout));
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/getSettings")
    public ResponseEntity<?> getSettings() {
        return new ResponseEntity<>(indexingJobManager.getSettings(), HttpStatus.OK);
    }

    @PostMapping(value = "/setSettings")
    public ResponseEntity<?> setSettings(@RequestParam String type, @RequestBody Map<String, Object> settings) {
        System.out.println(type);
        System.out.println(settings);
        if(type.equals("indexing")){
            indexingJobManager.setSettings("indexing", settings);
            return new ResponseEntity<>(HttpStatus.OK);
        }else if(type.equals("propagate")){
            indexingJobManager.setSettings("propagate", settings);
            return new ResponseEntity<>(HttpStatus.OK);
        }else{
            return new ResponseEntity<>(HttpStatus.NOT_ACCEPTABLE);
        }
    }

    /**
     * 도우미 메서드 영역
     * */

    enum IndexingActionType{
        ALL("all"),
        INDEXING("indexing"),
//        PROPAGATE("propagate"),
        EXPOSE("expose"),
        STOP_PROPAGATION("stop_propagation"), STOP_INDEXING("stop_indexing"), SUB_START("sub_start"),
        UNKNOWN("");

        private String action;

        IndexingActionType(String action){
            this.action = action;
        }

        public String getAction() {
            return action;
        }
    }

    private IndexingActionType getActionType(String action) throws IndexingJobFailureException{
        switch (action) {
            case "all":
                return IndexingActionType.ALL;
            case "indexing":
                return IndexingActionType.INDEXING;
            case "expose":
                return IndexingActionType.EXPOSE;
            case "stop_propagation":
                return IndexingActionType.STOP_PROPAGATION;
            case "stop_indexing":
                return IndexingActionType.STOP_INDEXING;
            case "sub_start":
                return IndexingActionType.SUB_START;
            default:
                throw new IndexingJobFailureException("Not Found action Type : " + action);
        }
    }

    private void handleError(String host, String port, String collectionName, String action, Map<String, Object> response){
        // Host 에러 처리
        if(host == null || host.equals("")){
            response.put("message", "host is not correct");
            response.put("result", "fail");
        }

        // Port 에러 처리
        if (port == null || port.equals("")) {
            response.put("message", "port is not correct");
            response.put("result", "fail");
        }

        // CollectionName 에러 처리
        if(collectionName == null || collectionName.equals("")){
            response.put("message", "collectionName is not correct");
            response.put("result", "fail");
        }

        // action 에러 처리
        if (action == null || action.equals("")) {
            response.put("message", "action is not correct");
            response.put("result", "fail");
        }

        // port가 Integer 범위를 넘어가면 에러
        try {
            Integer.parseInt(port);
        } catch (NumberFormatException e) {
            response.put("message", e.getMessage());
            response.put("result", "fail");
        }
    }

    private void registerIndexingJob(
            UUID clusterId,
            String id,
            Collection collection,
            IndexingActionType actionType,
            String groupSeq,
            Map<String, Object> response) throws IndexingJobFailureException, IOException {
        switch (actionType){
            case ALL:
                synchronized (obj) {
                    IndexingStatus registerStatus = indexingJobManager.findById(id);
                    if (registerStatus == null) {
                        Queue<IndexStep> nextStep = new ArrayDeque<>();
                        nextStep.add(IndexStep.EXPOSE);
                        IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, true, IndexStep.FULL_INDEX, nextStep);
                        indexingStatus.setStatus("RUNNING");
                        indexingJobManager.add(collection.getId(), indexingStatus);
                        response.put("indexingStatus", indexingStatus);
                        response.put("result", "success");
                    } else {
                        response.put("result", "fail");
                    }
                }
                break;
            case INDEXING:
                synchronized (obj) {
                    IndexingStatus registerStatus = indexingJobManager.findById(id);
                    if (registerStatus == null) {
                        IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, false, IndexStep.FULL_INDEX);
                        indexingStatus.setAction(actionType.getAction());
                        indexingStatus.setStatus("RUNNING");
                        indexingJobManager.add(collection.getId(), indexingStatus);
                        response.put("indexingStatus", indexingStatus);
                        response.put("result", "success");
                    } else {
                        response.put("result", "fail");
                    }
                }
                break;
            case EXPOSE:
                synchronized (obj) {
                    IndexingStatus registerStatus = indexingJobManager.findById(id);
                    if (registerStatus == null) {
                        indexingJobService.expose(clusterId, collection);
                        response.put("result", "success");
                    } else {
                        response.put("result", "fail");
                    }
                }
                break;
            case STOP_INDEXING:
                synchronized (obj) {
                    IndexingStatus indexingStatus = indexingJobManager.findById(id);
                    if (indexingStatus != null && (indexingStatus.getCurrentStep() == IndexStep.FULL_INDEX || indexingStatus.getCurrentStep() == IndexStep.DYNAMIC_INDEX)) {
                        Collection.Launcher launcher = collection.getLauncher();
                        indexingJobService.stopIndexing(indexingStatus.getScheme(), launcher.getHost(), launcher.getPort(), indexingStatus.getIndexingJobId());
                        response.put("indexingStatus", indexingStatus);
                        response.put("result", "success");
                        indexingJobManager.setStopStatus(id, "STOP"); // 추가
                    } else {
                        response.put("result", "fail");
                    }
                }
                break;
            case SUB_START:
                synchronized (obj) {
                    IndexingStatus indexingStatus = indexingJobManager.findById(id);
                    if (indexingStatus != null && (indexingStatus.getCurrentStep() == IndexStep.FULL_INDEX || indexingStatus.getCurrentStep() == IndexStep.DYNAMIC_INDEX) && groupSeq != null && !"".equalsIgnoreCase(groupSeq) ) {
                        logger.info("sub_start >>>> {}, groupSeq: {}", collection.getName(), groupSeq);
                        Collection.Launcher launcher = collection.getLauncher();
                        indexingJobService.subStart(indexingStatus.getScheme(), launcher.getHost(), launcher.getPort(), indexingStatus.getIndexingJobId(), groupSeq, collection.isExtIndexer());
                        response.put("indexingStatus", indexingStatus);
                        response.put("result", "success");
                    } else {
                        response.put("result", "fail");
                    }
                }
                break;
            default:
                response.put("message", "Not Found Action. Please Select Action in this list (all / indexing / propagate / expose / stop_indexing / stop_propagation)");
                response.put("result", "success");
        }
    }
}