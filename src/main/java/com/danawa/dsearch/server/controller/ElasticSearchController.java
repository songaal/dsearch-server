package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.ClusterStatusResponse;
import com.danawa.dsearch.server.entity.ClusterStatusRequest;
import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.services.ClusterService;
import com.danawa.dsearch.server.services.ElasticsearchProxyService;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/elasticsearch")
public class ElasticSearchController {
    private static Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);

    private ElasticsearchProxyService proxyService;
    private final ClusterService clusterService;

    public ElasticSearchController(ElasticsearchProxyService proxyService, ClusterService clusterService) {
        this.proxyService = proxyService;
        this.clusterService = clusterService;
    }

    @PostMapping("/status")
    public ResponseEntity<?> status(@RequestBody ClusterStatusRequest clusterStatusRequest) {
        ClusterStatusResponse clusterStatus = clusterService.scanClusterStatus(clusterStatusRequest);
        return new ResponseEntity<>(clusterStatus, HttpStatus.OK);
    }

    @RequestMapping({"/*", "/**/*"})
    public ResponseEntity<?> proxy(HttpServletRequest request,
                                   @RequestHeader(value = "cluster-id") UUID clusterId,
                                   @RequestParam Map<String,String> queryStringMap,
                                   @RequestBody(required = false) byte[] body) throws ElasticQueryException {
        try {
            Response response = proxyService.proxy(clusterId, request, queryStringMap, body);
            String responseBody = EntityUtils.toString(response.getEntity());
            return new ResponseEntity<>(responseBody, HttpStatus.OK);
        } catch (Exception e) {
            throw new ElasticQueryException(e);
        }
    }
}
