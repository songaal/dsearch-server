package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.services.IndexTemplateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/templates")
public class IndexTemplateController {

    @Autowired
    private IndexTemplateService service ;


    @GetMapping("/comments")
    public ResponseEntity<?> getTemplatesComment(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        return new ResponseEntity<>(service.getTemplateComment(clusterId), HttpStatus.OK);
    }

    @PutMapping("/comments")
    public ResponseEntity<?> setTemplatesComment(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                 @RequestBody Map<String, Object> map) throws IOException {

        service.setTemplateComment(clusterId, map);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
