package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.services.JdbcService;
import com.danawa.dsearch.server.services.PipelineService;
import org.apache.http.util.EntityUtils;
import org.apache.tomcat.util.json.JSONParser;
import org.elasticsearch.client.Response;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/pipeline")
public class PipelineController {
    private static Logger logger = LoggerFactory.getLogger(PipelineController.class);

    @Autowired
    private PipelineService pipelineService;

    public PipelineController() { }

    @GetMapping("/list")
    public ResponseEntity<?> getPipeLineLists(@RequestHeader(value = "cluster-id") UUID clusterId) throws Exception {
        Response pluginResponse = pipelineService.getPipeLineLists(clusterId);
        JSONParser parser = new JSONParser(EntityUtils.toString(pluginResponse.getEntity()));
        Object obj = parser.parse();
        Map<String, Object> jsonObj = (LinkedHashMap) obj;
        return new ResponseEntity<>(jsonObj, HttpStatus.OK);
    }

    @GetMapping("/{name}")
    public ResponseEntity<?> getPipeLineLists(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @PathVariable String name) throws Exception {
        Response response = pipelineService.getPipeLine(clusterId, name);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }

    @PutMapping("/{name}")
    public ResponseEntity<?> setPipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name,
                                         @RequestBody String body) throws Exception {
        Response response = pipelineService.setPipeLine(clusterId, name, body);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<?> deletePipeLine(@RequestHeader(value = "cluster-id") UUID clusterId,
                                         @PathVariable String name) throws Exception {
        Response response = pipelineService.deletePipeLine(clusterId, name);
        return new ResponseEntity<>(EntityUtils.toString(response.getEntity()), HttpStatus.OK);
    }
}
