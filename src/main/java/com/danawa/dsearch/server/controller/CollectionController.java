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
                                            @RequestBody Collection collection) throws IOException, DuplicateException {

        logger.info("action: {}, id: {}, collection: {}", action, id, collection);
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
        logger.debug("collection: {}, action: {}", id, action);
        if ("all".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    Collection collection = collectionService.findById(clusterId, id);
                    Queue<IndexStep> nextStep = new ArrayDeque<>();
                    nextStep.add(IndexStep.PROPAGATE);
                    nextStep.add(IndexStep.EXPOSE);
                    IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, true, IndexStep.FULL_INDEX, nextStep);
                    indexingJobManager.add(collection.getId(), indexingStatus);
                    response.put("indexingStatus", indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("indexing".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    Collection collection = collectionService.findById(clusterId, id);
                    IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, false, IndexStep.FULL_INDEX);
                    indexingJobManager.add(collection.getId(), indexingStatus);
                    response.put("indexingStatus", indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("propagate".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    Collection collection = collectionService.findById(clusterId, id);
                    IndexingStatus indexingStatus = indexingJobService.propagate(clusterId, false, collection, null);
                    indexingJobManager.add(collection.getId(), indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("expose".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    Collection collection = collectionService.findById(clusterId, id);
                    indexingJobService.expose(clusterId, collection);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("stop_propagation".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus != null && registerStatus.getCurrentStep() == IndexStep.PROPAGATE) {
                    registerStatus.setStatus("STOP");
                    Collection collection = collectionService.findById(clusterId, id);
                    indexingJobService.stopPropagation(clusterId, collection);
                    indexingJobManager.setStopStatus(id, "STOP"); // 추가
                    indexingJobManager.remove(id);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("stop_indexing".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus indexingStatus = indexingJobManager.findById(id);
                if (indexingStatus != null && (indexingStatus.getCurrentStep() == IndexStep.FULL_INDEX || indexingStatus.getCurrentStep() == IndexStep.DYNAMIC_INDEX)) {
                    indexingStatus.setStatus("STOP");
                    Collection collection = collectionService.findById(clusterId, id);
                    Collection.Launcher launcher = collection.getLauncher();
                    indexingJobService.stopIndexing(indexingStatus.getScheme(), launcher.getHost(), launcher.getPort(), indexingStatus.getIndexingJobId());
                    indexingJobManager.setStopStatus(id, "STOP"); // 추가
                    response.put("indexingStatus", indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/idxp")
    public ResponseEntity<?> idxp(@RequestParam String host,
                                      @RequestParam String port,
                                      @RequestParam String collectionName,
                                      @RequestParam String action) throws IndexingJobFailureException, IOException {
        Map<String, Object> response = new HashMap<>();

        // Host 에러 처리
        if(host == null || host.equals("")){
            response.put("message", "host is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        // Port 에러 처리
        if (port == null || port.equals("")) {
            response.put("message", "port is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        // CollectionName 에러 처리
        if(collectionName == null || collectionName.equals("")){
            response.put("message", "collectionName is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        // action 에러 처리
        if (action == null || action.equals("")) {
            response.put("message", "action is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        int parsePort = 0;
        try {
            parsePort = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            response.put("message", e.getMessage());
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

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
        logger.info(collection.toString());
        if(collection == null){
            //컬렉션이 없을때
            response.put("message", "Not Found Collection Name");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        UUID clusterId = cluster.getId();
        String id = collection.getId();

        if ("all".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    Queue<IndexStep> nextStep = new ArrayDeque<>();
                    nextStep.add(IndexStep.PROPAGATE);
                    nextStep.add(IndexStep.EXPOSE);
                    IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, true, IndexStep.FULL_INDEX, nextStep);
                    indexingJobManager.add(collection.getId(), indexingStatus);
                    response.put("indexingStatus", indexingStatus);
                    indexingStatus.setStatus("RUNNING");
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("indexing".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, false, IndexStep.FULL_INDEX);
                    indexingJobManager.add(collection.getId(), indexingStatus);
                    indexingStatus.setAction(action);
                    indexingStatus.setStatus("RUNNING");
                    response.put("indexingStatus", indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("propagate".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    IndexingStatus indexingStatus = indexingJobService.propagate(clusterId, false, collection, null);
                    indexingStatus.setStatus("RUNNING");
                    indexingStatus.setAction(action);
                    indexingJobManager.add(collection.getId(), indexingStatus);

                    response.put("indexingStatus", indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("expose".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus == null) {
                    indexingJobService.expose(clusterId, collection);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("stop_propagation".equalsIgnoreCase(action)) {
            synchronized (obj) {
                IndexingStatus registerStatus = indexingJobManager.findById(id);
                if (registerStatus != null && registerStatus.getCurrentStep() == IndexStep.PROPAGATE) {
                    indexingJobService.stopPropagation(clusterId, collection);
                    indexingJobManager.setStopStatus(id, "STOP"); // 추가
                    indexingJobManager.remove(id);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        } else if ("stop_indexing".equalsIgnoreCase(action)) {
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
        } else {
            response.put("message", "Not Found Action. Please Select Action in this list (all / indexing / propagate / expose / stop_indexing / stop_propagation)");
            response.put("result", "success");
        }

        return new ResponseEntity<>(response, HttpStatus.OK);
    }


    @GetMapping("/idxp/status")
    public ResponseEntity<?> getIdxpStatus(@RequestParam String host,
                                  @RequestParam String port,
                                  @RequestParam String collectionName) throws IndexingJobFailureException, IOException {
        Map<String, Object> response = new HashMap<>();

        // 에러 처리
        if(host == null || host.equals("")){
            response.put("message", "host is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        if (port == null || port.equals("")) {
            response.put("message", "port is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        if(collectionName == null || collectionName.equals("")){
            response.put("message", "collectionName is not correct");
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

        int parsePort = 0;
        try {
            parsePort = Integer.parseInt(port);
        } catch (NumberFormatException e) {
            response.put("message", e.getMessage());
            response.put("result", "fail");
            return new ResponseEntity<>(response, HttpStatus.OK);
        }

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
}