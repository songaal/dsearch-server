package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.ChangeIndexRequset;
import com.danawa.fastcatx.server.entity.Collection;
import com.danawa.fastcatx.server.excpetions.DuplicateException;
import com.danawa.fastcatx.server.services.CollectionService;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/collections")
public class CollectionController {
    private static Logger logger = LoggerFactory.getLogger(CollectionController.class);

    private final String indexSuffixA;
    private final String indexSuffixB;
    private final CollectionService collectionService;

    public CollectionController(@Value("${fastcatx.collection.index-suffix-a}") String indexSuffixA,
                                @Value("${fastcatx.collection.index-suffix-b}") String indexSuffixB,
                                CollectionService collectionService) {
        this.indexSuffixA = indexSuffixA;
        this.indexSuffixB = indexSuffixB;
        this.collectionService = collectionService;
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
}
