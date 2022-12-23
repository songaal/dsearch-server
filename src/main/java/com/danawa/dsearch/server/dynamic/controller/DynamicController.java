package com.danawa.dsearch.server.dynamic.controller;

import com.danawa.dsearch.server.dynamic.service.DynamicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;

@RestController
@RequestMapping("/dynamic")
public class DynamicController {
    private static Logger logger = LoggerFactory.getLogger(DynamicController.class);

    private DynamicService dynamicService;

    public DynamicController(DynamicService dynamicService) {
        this.dynamicService = dynamicService;
    }

    @GetMapping("/find")
    public ResponseEntity<?> getFindAll() {
        return new ResponseEntity<>(dynamicService.findAll(), HttpStatus.OK);
    }

    @GetMapping("/bundle/find")
    public ResponseEntity<?> getBundleFindAll(@RequestParam(value = "desc", defaultValue = "queue") String desc) {
        return new ResponseEntity<>(dynamicService.findBundleAll(desc), HttpStatus.OK);
    }

    @GetMapping("/state/{id}")
    public ResponseEntity<?> getState(@PathVariable int id) {
        return new ResponseEntity<>(dynamicService.state(id), HttpStatus.OK);
    }

    @PutMapping("/upload")
    public ResponseEntity<?> getFileUpload(@RequestBody HashMap<String, Object> body) {
        return new ResponseEntity<>(dynamicService.fileUpload(body), HttpStatus.OK);
    }

    @PutMapping("/consume-all/{id}")
    public ResponseEntity<?> consumeAll(@PathVariable int id, @RequestBody HashMap<String, Object> body) throws IOException {
        return new ResponseEntity<>(dynamicService.consumeAll(id, body), HttpStatus.OK);
    }

    @GetMapping("/file/read")
    public ResponseEntity<?> fileRead() {
        return new ResponseEntity<>(dynamicService.fileRead(), HttpStatus.OK);
    }
}
