package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.ChangeIndexRequset;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.entity.IndexStep;
import com.danawa.fastcatx.server.entity.IndexingStatus;
import com.danawa.fastcatx.server.excpetions.DuplicateException;
import com.danawa.fastcatx.server.excpetions.IndexingJobFailureException;
import com.danawa.fastcatx.server.services.CollectionService;
import com.danawa.fastcatx.server.services.IndexingJobManager;
import com.danawa.fastcatx.server.services.IndexingJobService;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

@RestController
@RequestMapping("/collections")
public class CollectionController {
    private static Logger logger = LoggerFactory.getLogger(CollectionController.class);

    private static Object obj = new Object();
    private final String indexSuffixA;
    private final String indexSuffixB;
    private final CollectionService collectionService;
    private final IndexingJobService indexingJobService;
    private final IndexingJobManager indexingJobManager;

    public CollectionController(@Value("${fastcatx.collection.index-suffix-a}") String indexSuffixA,
                                @Value("${fastcatx.collection.index-suffix-b}") String indexSuffixB,
                                CollectionService collectionService,
                                IndexingJobService indexingJobService, IndexingJobManager indexingJobManager) {
        this.indexSuffixA = indexSuffixA;
        this.indexSuffixB = indexSuffixB;
        this.collectionService = collectionService;
        this.indexingJobService = indexingJobService;
        this.indexingJobManager = indexingJobManager;
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
                    Collection collection = collectionService.findById(clusterId, id);
                    indexingJobService.stopPropagation(clusterId, collection);
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
                    Collection collection = collectionService.findById(clusterId, id);
                    Collection.Launcher launcher = collection.getLauncher();
                    indexingJobService.stopIndexing(launcher.getHost(), launcher.getPort(), indexingStatus.getIndexingJobId());
                    response.put("indexingStatus", indexingStatus);
                    response.put("result", "success");
                } else {
                    response.put("result", "fail");
                }
            }
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}