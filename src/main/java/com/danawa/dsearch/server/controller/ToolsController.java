package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.DetailAnalysisRequest;
import com.danawa.dsearch.server.services.ToolsService;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/tools")
public class ToolsController {
    private static Logger logger = LoggerFactory.getLogger(ToolsController.class);

    @Autowired
    ToolsService toolsService;

    public ToolsController() { }

    @GetMapping("/plugins")
    public ResponseEntity<?> getPlugins(@RequestHeader(value = "cluster-id") UUID clusterId) throws Exception {
        Map<String, Object> resultEntitiy = new HashMap<String, Object>();

        Response pluginResponse = toolsService.getPlugins(clusterId);
        String responseBody = EntityUtils.toString(pluginResponse.getEntity());
        String[] splitLists = responseBody.split("\n");
        Set<String> plugins = new HashSet<>();
        for(String list : splitLists){
            String[] items  = list.replaceAll(" +", " ").split(" ");

            if(items.length >= 2)
                plugins.add(items[1]);
        }

        List<String> result = toolsService.checkPlugins(clusterId, plugins);
        resultEntitiy.put("plugins", result);
        return new ResponseEntity<>(resultEntitiy, HttpStatus.OK);
    }

    @PostMapping("/detail/analysis")
    public ResponseEntity<?> getDetailAnalysis(@RequestHeader(value = "cluster-id") UUID clusterId,
                                               @RequestBody DetailAnalysisRequest detailAnalysisRequest) throws Exception {
        Response response = toolsService.getDetailAnalysis(clusterId, detailAnalysisRequest);
        String responseBody = EntityUtils.toString(response.getEntity());
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }
}

