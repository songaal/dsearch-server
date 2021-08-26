package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.*;
import com.danawa.dsearch.server.excpetions.NotFoundException;
import com.danawa.dsearch.server.services.ClusterService;
import com.danawa.dsearch.server.services.ProxyService;
import com.danawa.dsearch.server.services.ReferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/reference")
public class ReferenceController {
    private static Logger logger = LoggerFactory.getLogger(ReferenceController.class);

    private ReferenceService referenceService;
    private ClusterService clusterService;
    private ProxyService proxyService ;

    public ReferenceController(ReferenceService referenceService, ClusterService clusterService,ProxyService proxyService ) {
        this.referenceService = referenceService;
        this.clusterService = clusterService;
        this.proxyService = proxyService;
    }

    @GetMapping
    public ResponseEntity<?> findAll(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        List<ReferenceTemp> tempList = referenceService.findAll(clusterId);
        return new ResponseEntity<>(tempList, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable String id,
                                  @RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        ReferenceTemp temp = referenceService.find(clusterId, id);
        return new ResponseEntity<>(temp, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestHeader(value = "cluster-id") UUID clusterId,
                                 @RequestBody ReferenceTemp entity) throws IOException {
        referenceService.add(clusterId, entity);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/actions")
    public ResponseEntity<?> actions(@RequestHeader(value = "cluster-id") UUID clusterId,
                                     @RequestParam String action,
                                     @RequestBody(required = false) ReferenceOrdersRequest request) throws IOException {
        if ("orders".equalsIgnoreCase(action)) {
            referenceService.updateOrders(clusterId, request);
        }

        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @PathVariable String id,
                                    @RequestBody ReferenceTemp entity) throws IOException {
        referenceService.update(clusterId, id, entity);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @PathVariable String id) throws IOException {
        referenceService.delete(clusterId, id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/_search")
    public ResponseEntity<?> searchResponseAll(@RequestHeader(value = "cluster-id") UUID clusterId,
                                               @RequestParam String keyword) throws IOException {
        List<ReferenceResult> result = referenceService.searchResponseAll(clusterId, keyword);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping("/{id}/_search")
    public ResponseEntity<?> searchResponse(@PathVariable String id,
                                            @RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestParam String keyword,
                                            @RequestParam long pageNum,
                                            @RequestParam long rowSize) throws IOException {
        ReferenceResult result = referenceService.searchResponse(clusterId, id, keyword, pageNum, rowSize);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping("/save/autocomplete")
    public ResponseEntity<?> saveAutoCompleteURL(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                 @RequestBody Map<String, Object> request) throws NotFoundException {

        String url = (String) Objects.requireNonNull(request.get("url"));
        Cluster cluster = clusterService.find(clusterId);
        Cluster saveCluster = clusterService.saveUrl(clusterId, cluster, url);
        Map<String, Object> result = new HashMap<>();

        if(saveCluster != null){
            result.put("isSuccess", true);
            result.put("url", url);
        }else{
            result.put("isSuccess", false);
            result.put("url", url);
        }
        return new ResponseEntity<>(result, HttpStatus.OK);
    }
    @GetMapping("/get/autocomplete")
    public ResponseEntity<?> getAutoCompleteURL(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException, NotFoundException {
        Cluster cluster = clusterService.find(clusterId);

        Map<String, Object> result = new HashMap<>();
            if(cluster.getAutocompleteUrl() == null || "".equals(cluster.getAutocompleteUrl())){
                result.put("url", "");
            }else{
                result.put("url", cluster.getAutocompleteUrl());
            }System.out.println(result.get("url"));
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @GetMapping("/autocomplete")
    public ResponseEntity<?> getAutoComplete(HttpServletRequest request,
                                             @RequestHeader(value = "cluster-id") UUID clusterId,
                                             @RequestParam Map<String,String> queryStringMap) throws IOException, NotFoundException {
        Cluster cluster = clusterService.find(clusterId);
        String keyword = null;
        for(String key: queryStringMap.keySet()){
            keyword = key;
            break;
        }
        if (cluster.getAutocompleteUrl() != null) {
            ResponseEntity responseEntity = proxyService.proxy(cluster.getAutocompleteUrl(), keyword);
            return responseEntity;
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}
