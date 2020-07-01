package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.ReferenceOrdersRequest;
import com.danawa.fastcatx.server.entity.ReferenceResult;
import com.danawa.fastcatx.server.entity.ReferenceTemp;
import com.danawa.fastcatx.server.services.ReferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/reference")
public class ReferenceController {
    private static Logger logger = LoggerFactory.getLogger(ReferenceController.class);

    private ReferenceService referenceService;

    public ReferenceController(ReferenceService referenceService) {
        this.referenceService = referenceService;
    }

    @GetMapping
    public ResponseEntity<?> findAll(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        List<ReferenceTemp> tempList = referenceService.findAll(clusterId);
        return new ResponseEntity<>(tempList, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable String id,
                                  @RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        ReferenceTemp temp = referenceService.find(clusterId, id);
        return new ResponseEntity<>(temp, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestHeader(value = "cluster-id") UUID clusterId,
                                 @RequestBody ReferenceTemp entity) throws IOException {
        referenceService.add(clusterId, entity);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/actions")
    public ResponseEntity<?> actions(@RequestHeader(value = "cluster-id") UUID clusterId,
                                     @RequestParam String action,
                                     @RequestBody(required = false) ReferenceOrdersRequest request) throws IOException {
        if ("orders".equalsIgnoreCase(action)) {
            referenceService.updateOrders(clusterId, request);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @PathVariable String id,
                                    @RequestBody ReferenceTemp entity) throws IOException {
        referenceService.update(clusterId, id, entity);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @PathVariable String id) throws IOException {
        referenceService.delete(clusterId, id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/_search")
    public ResponseEntity<?> searchResponseAll(@RequestHeader(value = "cluster-id") UUID clusterId,
                                               @RequestParam String keyword) throws IOException {
        List<ReferenceResult> result = referenceService.searchResponseAll(clusterId, keyword);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping("/{id}/_search")
    public ResponseEntity<?> searchResponse(@PathVariable String id,
                                            @RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestParam String keyword,
                                            @RequestParam long pageNum,
                                            @RequestParam long rowSize) throws IOException {
        ReferenceResult result = referenceService.searchResponse(clusterId, id, keyword, pageNum, rowSize);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

}
