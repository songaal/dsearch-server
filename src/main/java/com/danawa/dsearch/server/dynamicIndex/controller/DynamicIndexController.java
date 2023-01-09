package com.danawa.dsearch.server.dynamicIndex.controller;

import com.danawa.dsearch.server.dynamicIndex.dto.DynamicIndexInfoRequest;
import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import com.danawa.dsearch.server.dynamicIndex.service.DynamicIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/dynamicIndex")
public class DynamicIndexController {
    private static Logger logger = LoggerFactory.getLogger(DynamicIndexController.class);

    private DynamicIndexService dynamicIndexService;

    public DynamicIndexController(DynamicIndexService dynamicIndexService) {
        this.dynamicIndexService = dynamicIndexService;
    }

    @GetMapping
    public ResponseEntity<?> getFindAll() {
        return new ResponseEntity<>(dynamicIndexService.findAll(), HttpStatus.OK);
    }

    @GetMapping("/bundle")
    public ResponseEntity<?> getBundleFindAll() {
        return new ResponseEntity<>(dynamicIndexService.findBundleAll(), HttpStatus.OK);
    }

    @GetMapping("/state/{id}")
    public ResponseEntity<?> getState(@PathVariable int id) {
        return new ResponseEntity<>(dynamicIndexService.getState(id), HttpStatus.OK);
    }

    @GetMapping("/state")
    public ResponseEntity<?> getStateAll() {
        return new ResponseEntity<>(dynamicIndexService.getStateAll(), HttpStatus.OK);
    }

    @PutMapping("/consume-all/{id}")
    public ResponseEntity<?> consumeAll(@PathVariable int id, @RequestBody HashMap<String, Object> body) throws IOException {
        return new ResponseEntity<>(dynamicIndexService.consumeAll(id, body), HttpStatus.OK);
    }

    @PutMapping
    public ResponseEntity<?> resetAll(@RequestBody List<DynamicIndexInfoRequest> body) {
        List<DynamicIndexInfo> list = changeToDynamicIndexInfoList(body);
        return new ResponseEntity<>(dynamicIndexService.resetAll(list), HttpStatus.OK);
    }

    private List<DynamicIndexInfo> changeToDynamicIndexInfoList(List<DynamicIndexInfoRequest> infoList){
        List<DynamicIndexInfo> result = new ArrayList<>();
        for(DynamicIndexInfoRequest request: infoList){
            result.add(DynamicIndexInfoRequest.to(request));
        }
        return result;
    }
}
