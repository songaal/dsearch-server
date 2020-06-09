package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.entity.ReferenceOrdersRequest;
import com.danawa.fastcatx.server.entity.ReferenceResult;
import com.danawa.fastcatx.server.entity.ReferenceTemp;
import com.danawa.fastcatx.server.services.ReferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin("*")
@RequestMapping("/reference")
public class ReferenceController {
    private static Logger logger = LoggerFactory.getLogger(ReferenceController.class);

    private ReferenceService referenceService;

    public ReferenceController(ReferenceService referenceService) {
        this.referenceService = referenceService;
    }

    @GetMapping
    public ResponseEntity<?> findAll() throws IOException {
        List<ReferenceTemp> tempList = referenceService.findAll();
        return new ResponseEntity<>(tempList, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable String id) throws IOException {
        ReferenceTemp temp = referenceService.find(id);
        return new ResponseEntity<>(temp, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody ReferenceTemp entity) throws IOException {
        referenceService.add(entity);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/actions")
    public ResponseEntity<?> actions(@RequestParam String action,
                                     @RequestBody(required = false) ReferenceOrdersRequest request) throws IOException {
        if ("orders".equalsIgnoreCase(action)) {
            referenceService.updateOrders(request);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable String id,
                                    @RequestBody ReferenceTemp entity) throws IOException {
        referenceService.update(id, entity);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable String id) throws IOException {
        referenceService.delete(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/_search")
    public ResponseEntity<?> searchResponseAll(@RequestParam String keyword) throws IOException {
        List<ReferenceResult> result = referenceService.searchResponseAll(keyword);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping("/{id}/_search")
    public ResponseEntity<?> searchResponse(@PathVariable String id,
                                            @RequestParam String keyword,
                                            @RequestParam(defaultValue = "0") long pageNum,
                                            @RequestParam(defaultValue = "100") long rowSize) throws IOException {
        ReferenceResult result = referenceService.searchResponse(id, keyword, pageNum, rowSize);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

}
