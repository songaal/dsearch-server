package com.danawa.dsearch.server.templates.controller;

import com.danawa.dsearch.server.templates.service.IndexTemplateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/templates")
public class IndexTemplateController {
    private static Logger logger = LoggerFactory.getLogger(IndexTemplateController.class);
    private IndexTemplateService service ;

    public IndexTemplateController( IndexTemplateService service){
        this.service = service;
    }
    @GetMapping("/comments")
    public ResponseEntity<?> getTemplatesComment(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        return new ResponseEntity<>(service.getTemplateComment(clusterId), HttpStatus.OK);
    }
    @PutMapping("/comments")
    public ResponseEntity<?> setTemplatesComment(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                 @RequestBody Map<String, Object> map) throws IOException {
        service.saveTemplateComment(clusterId, map);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
