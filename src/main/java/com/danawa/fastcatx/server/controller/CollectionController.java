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
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

    @PostMapping("/change")
    public ResponseEntity<?> changeIndex(@RequestHeader(value = "cluster-id") UUID clusterId, @RequestBody ChangeIndexRequset changeIndexRequset) throws IOException{
        Response response = collectionService.changeIndex(clusterId, changeIndexRequset);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }



    @PostMapping("/propagate")
    public ResponseEntity<?> propagateIndex(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        // 1. index가 Success 인지 확인

//        if(index == success?){
//            replica:1, index.routing.allocation.include._exclude=index* 호출
//          return new ResponseEntity<>(HttpStatus.OK);
//        }else{
//        Map<String, Object> response = new HashMap<>();
//        response.put("status", "Not Success");
//        response.put("statusPercent", ???);
//        response.put("statusCode", 0);
//         return new ResponseEntity<>("", HttpStatus.OK);
//    }

        // 2.

        return new ResponseEntity<>(HttpStatus.OK);
    }

    
    @PutMapping("/{id}/action")
    public ResponseEntity<?> indexing(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @PathVariable String id,
                                      @RequestParam String action) throws IOException, IndexingJobFailureException {
            if ("indexing".equalsIgnoreCase(action)) {
                synchronized (obj) {
                    IndexingStatus registerStatus = indexingJobManager.findById(id);
                    if (registerStatus == null) {
                        Collection collection = collectionService.findById(clusterId, id);
                        IndexingStatus indexingStatus = indexingJobService.indexing(clusterId, collection, false, IndexStep.FULL_INDEX);
                        indexingJobManager.add(collection.getId(), indexingStatus);
                    }
                }
            } else if ("propagate".equalsIgnoreCase(action)) {
                synchronized (obj) {
                    Collection collection = collectionService.findById(clusterId, id);
                    IndexingStatus indexingStatus = indexingJobService.propagate(clusterId, false, collection);
                    indexingJobManager.add(collection.getId(), indexingStatus);
                }
            }
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
