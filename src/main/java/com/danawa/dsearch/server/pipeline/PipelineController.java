package com.danawa.dsearch.server.pipeline;

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
        Response pluginResponse = pipelineService.getPipeLineLists(clusterId);
        return new ResponseEntity<>(EntityUtils.toString(pluginResponse.getEntity()), HttpStatus.OK);
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> getPipeLineLists(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @PathVariable String name) throws Exception {
        Response response = pipelineService.getPipeLine(clusterId, name);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }

    @PutMapping(value = "/{name}")
    public ResponseEntity<?> setPipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name,
                                         @RequestBody HashMap<String, Object> body) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Response response = pipelineService.addPipeLine(clusterId, name, mapper.writeValueAsString(body));
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }

    @PostMapping(value = "/{name}")
    public ResponseEntity<?> testPipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name,
                                         @RequestParam boolean isDetail,
                                         @RequestBody HashMap<String, Object> body) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Response response = pipelineService.testPipeline(clusterId, name, mapper.writeValueAsString(body), isDetail);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> deletePipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name) throws Exception {
        Response response = pipelineService.deletePipeLine(clusterId, name);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }
}
