package com.danawa.dsearch.server.dynamicIndex.controller;

import com.danawa.dsearch.server.dynamicIndex.entity.BundleDescription;
import com.danawa.dsearch.server.dynamicIndex.service.DynamicIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;

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
    public ResponseEntity<?> getBundleFindAll(@RequestParam(value = "desc", defaultValue = "queue") String desc) {
        BundleDescription descriptionType = BundleDescription.getDescription(desc);
        return new ResponseEntity<>(dynamicIndexService.findBundleAll(descriptionType), HttpStatus.OK);
    }

    @GetMapping("/state/{id}")
    public ResponseEntity<?> getState(@PathVariable int id) {
        return new ResponseEntity<>(dynamicIndexService.state(id), HttpStatus.OK);
    }

    @GetMapping("/state")
    public ResponseEntity<?> getStateAll() {
        return new ResponseEntity<>(dynamicIndexService.stateAll(), HttpStatus.OK);
    }

//    @PutMapping
//    public ResponseEntity<?> getFileUpload(@RequestBody HashMap<String, Object> body) {
//        return new ResponseEntity<>(dynamicIndexService.fileUpload(body), HttpStatus.OK);
//    }

    @PutMapping("/consume-all/{id}")
    public ResponseEntity<?> consumeAll(@PathVariable int id, @RequestBody HashMap<String, Object> body) throws IOException {
        return new ResponseEntity<>(dynamicIndexService.consumeAll(id, body), HttpStatus.OK);
    }

//    @GetMapping("/file/read")
//    public ResponseEntity<?> fileRead() {
//        return new ResponseEntity<>(dynamicIndexService.fileRead(), HttpStatus.OK);
//    }
}
