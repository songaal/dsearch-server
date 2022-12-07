package com.danawa.dsearch.server.collections.controller;

import com.danawa.dsearch.server.collections.dto.HistoryDeleteRequest;
import com.danawa.dsearch.server.collections.dto.HistoryReadRequest;
import com.danawa.dsearch.server.collections.service.history.IndexHistoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/history")
public class CollectionHistoryController {

    private static Logger logger = LoggerFactory.getLogger(CollectionHistoryController.class);
    private IndexHistoryService indexHistoryService;

    public CollectionHistoryController(IndexHistoryService indexHistoryService){
        this.indexHistoryService = indexHistoryService;
    }


    @PostMapping
    public ResponseEntity<?> getHistory(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @RequestBody HistoryReadRequest historyReadRequest) {

        Map<String, Object> response = new HashMap<>();
        List<Map<String, Object>> list = indexHistoryService.findByCollection(clusterId, historyReadRequest);
        response.put("total_size", indexHistoryService.getTotalSize(clusterId, historyReadRequest));
        response.put("result", list);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping
    public ResponseEntity<?> deleteCollectionHistory(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                     @RequestBody HistoryDeleteRequest deleteRequest) {

        Map<String, Object> response = new HashMap<>();
        logger.info("{} {}", clusterId, deleteRequest);
        indexHistoryService.delete(clusterId, deleteRequest.getCollectionName());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
