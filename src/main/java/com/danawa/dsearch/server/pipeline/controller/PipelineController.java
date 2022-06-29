package com.danawa.dsearch.server.pipeline.controller;

import com.danawa.dsearch.server.pipeline.service.PipelineService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

@RestController
@RequestMapping("/pipeline")
public class PipelineController {
    private static Logger logger = LoggerFactory.getLogger(PipelineController.class);
    private final PipelineService pipelineService;
    public PipelineController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @GetMapping("/list")
    public ResponseEntity<?> getPipeLineLists(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        String response = pipelineService.getPipeLineLists(clusterId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> getPipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @PathVariable String name) throws Exception {
        String response = pipelineService.getPipeLine(clusterId, name);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PutMapping(value = "/{name}")
    public ResponseEntity<?> addPipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name,
                                         @RequestBody HashMap<String, Object> body) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String response = pipelineService.addPipeLine(clusterId, name, mapper.writeValueAsString(body));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping(value = "/{name}")
    public ResponseEntity<?> testPipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name,
                                         @RequestParam boolean isDetail,
                                         @RequestBody HashMap<String, Object> body) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String response = pipelineService.testPipeline(clusterId, name, mapper.writeValueAsString(body), isDetail);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> deletePipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name) throws Exception {
        String response = pipelineService.deletePipeLine(clusterId, name);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
