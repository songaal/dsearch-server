package com.danawa.dsearch.server.collections.controller;

import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.collections.entity.IndexingActionType;
import com.danawa.dsearch.server.collections.service.CollectionScheduleManager;
import com.danawa.dsearch.server.collections.service.IndexingJobManager;
import com.danawa.dsearch.server.collections.service.IndexingJobService;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.excpetions.DuplicatedUserException;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
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
    private final IndexingJobService indexingJobService;
    private final IndexingJobManager indexingJobManager;
    private final CollectionScheduleManager scheduleManager;

    public CollectionController(@Value("${dsearch.collection.index-suffix-a}") String indexSuffixA,
                                @Value("${dsearch.collection.index-suffix-b}") String indexSuffixB,
                                CollectionService collectionService,
                                ClusterService clusterService,
                                IndexingJobService indexingJobService,
                                IndexingJobManager indexingJobManager,
                                CollectionScheduleManager scheduleManager) {
        this.indexSuffixA = indexSuffixA;
        this.indexSuffixB = indexSuffixB;
        this.collectionService = collectionService;
        this.indexingJobService = indexingJobService;
        this.indexingJobManager = indexingJobManager;
        this.clusterService = clusterService;
        this.scheduleManager = scheduleManager;
    }

    @PostMapping
    public ResponseEntity<?> addCollection(@RequestHeader(value = "cluster-id") UUID clusterId,
                                           @RequestBody Collection collection) throws IOException, DuplicatedUserException {
        System.out.println(collection);
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
        return new ResponseEntity<>(indexingJobManager.getScheduleQueue(id), HttpStatus.OK);
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
        logger.info("action: {}, collectionId: {}, baseId: {}", action, id, collection.getBaseId());

        if ("source".equalsIgnoreCase(action)) {
            Map<String, Object> source = collectionService.editSource(clusterId, id, collection);

            boolean isScheduled = (boolean) source.get("scheduled");
            if(isScheduled){
                // collection에는 따로 셋팅해서 넘겨준다. -> sourceAsMap에는 있지만 Collection에는 false로 등록되어 있을 수 있기 때문에.
//                collection.setScheduled(true);
                scheduleManager.resetCollectionSchedule(clusterId, collection.getId()); // 스케줄 리셋
            }
        } else if ("schedule".equalsIgnoreCase(action)) {
            collectionService.editSchedule(clusterId, id, collection);
            scheduleManager.resetCollectionSchedule(clusterId, collection.getId()); // 스케줄이 업데이트 되었으므로 기존 데이터 삭제 후 재등록 필요

            logger.info("등록된 스케쥴 리스트");
            List<String> jobs = scheduleManager.getScheduledJobs();
            Collections.sort(jobs);
            for (String job: jobs){
                logger.info("{}", job);
            }
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}/action")
    public ResponseEntity<?> indexing(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @RequestHeader(value = "client-ip", required = false) String clientIP,
                                      @PathVariable String id,
                                      @RequestParam String action) throws IndexingJobFailureException, IOException {
        Map<String, Object> response = new HashMap<>();
        IndexingActionType actionType = getActionType(action);
        Collection collection = collectionService.findById(clusterId, id);
        collectionService.registerIndexingJob(clusterId, clientIP, id, collection, actionType, "", response);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/idxp")
    public ResponseEntity<?> idxp(@RequestParam(name = "host") String host,
                                      @RequestParam(name = "port") int port,
                                      @RequestParam(name = "collectionName") String collectionName,
                                      @RequestParam(required = false) String groupSeq,
                                      @RequestParam(name = "action") String action) throws IndexingJobFailureException, IOException {
        Map<String, Object> response = new HashMap<>();
        handleError(host, port, collectionName, action, response);

        List<Cluster> clusterList = clusterService.findByHostAndPort(host, port);
        if(clusterList.size() == 0){
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

        UUID clusterId = cluster.getId();
        String id = collection.getId();
        IndexingActionType actionType = getActionType(action);

//        IDXP에서 groupSeq가 넘어오면서 전체 색인을 시작한다.
//        일반적으로 관리도구 설정과 동일할거같지만... IDXP 파라미터가 있을 경우 groupSeq 를 변경한다.
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

        collectionService.registerIndexingJob(clusterId, "from-remote-indexer", id, collection, actionType, groupSeq, response);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @GetMapping("/idxp/status")
    public ResponseEntity<?> getIdxpStatus(@RequestParam(name = "host") String host,
                                  @RequestParam(name = "port") int port,
                                  @RequestParam(name = "collectionName") String collectionName) throws IOException {
        Map<String, Object> response = new HashMap<>();
        handleError(host, port, collectionName, "empty", response);

        List<Cluster> clusterList = clusterService.findByHostAndPort(host, port);
        if(clusterList.size() == 0){
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

        String collectionId = collection.getId();
        IndexingStatus indexingStatus = indexingJobManager.getIndexingStatus(collectionId);

        if(indexingStatus == null){
            Map<String, String> map = new HashMap<>();
            map.put("status", "NOT_STARTED");
            response.put("message", "Not Found Status (색인을 시작하지 않았습니다)");
            response.put("result", "success");
            response.put("info", map);
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        response.put("message", "");
        response.put("info",  indexingStatus);
        response.put("step",  indexingStatus.getCurrentStep());
        response.put("result", "success");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/schedule")
    public ResponseEntity<?> getScheduleQueue(){
        Map<String, Object> response = new HashMap<>();
        List<IndexingStatus> statusList = indexingJobManager.getScheduleQueueList();
        response.put("message", "");
        response.put("data",  statusList);
        response.put("result", "success");
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/lookup")
    public ResponseEntity<?> getLookupQueue(){
        Map<String, Object> response = new HashMap<>();
        List<IndexingStatus> statusList = indexingJobManager.getLookupQueueList();
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
     * 도우미 메서드 영역
     * */
    private IndexingActionType getActionType(String action) throws IndexingJobFailureException{
        action = action.toLowerCase();
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
            case "stop_reindexing":
                return IndexingActionType.STOP_REINDEXING;
            default:
                throw new IndexingJobFailureException("Not Found action Type : " + action);
        }
    }

    private void handleError(String host, int port, String collectionName, String action, Map<String, Object> response){
        // Host 에러 처리
        if(host == null || host.equals("")){
            response.put("message", "host is not correct");
            response.put("result", "fail");
        }

        // Port 에러 처리
        if (port <= 0) {
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
    }
}