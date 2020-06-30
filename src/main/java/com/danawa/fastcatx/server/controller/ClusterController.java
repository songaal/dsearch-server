package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.Cluster;
import com.danawa.fastcatx.server.entity.ClusterStatusResponse;
import com.danawa.fastcatx.server.excpetions.NotFoundException;
import com.danawa.fastcatx.server.excpetions.NotFoundUserException;
import com.danawa.fastcatx.server.services.ClusterService;
import com.danawa.fastcatx.server.services.DictionaryService;
import com.danawa.fastcatx.server.services.ReferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/clusters")
public class ClusterController {
    private static Logger logger = LoggerFactory.getLogger(ClusterController.class);

    private final ClusterService clusterService;
    private final DictionaryService dictionaryService;
    private final ReferenceService referenceService;

    public ClusterController(ClusterService clusterService, DictionaryService dictionaryService, ReferenceService referenceService) {
        this.clusterService = clusterService;
        this.dictionaryService = dictionaryService;
        this.referenceService = referenceService;
    }

    @GetMapping
    public ResponseEntity<?> findAll() {
        List<Map<String, Object>> response = new ArrayList<>();
        List<Cluster> clusterList = clusterService.findAll();
        int size = clusterList.size();
        for (int i=0; i < size; i++) {
            Cluster cluster = clusterList.get(i);
            ClusterStatusResponse status = clusterService.scanClusterStatus(cluster.getScheme(),
                    cluster.getHost(),
                    cluster.getPort(),
                    cluster.getUsername(),
                    cluster.getPassword());
            Map<String, Object> clusterMap = new HashMap<>();
            clusterMap.put("cluster", cluster);
            clusterMap.put("status", status);
            response.add(clusterMap);
        }
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable String id) {
        Cluster cluster = clusterService.find(UUID.fromString(id));
        ClusterStatusResponse status = clusterService.scanClusterStatus(cluster.getScheme(),
                cluster.getHost(),
                cluster.getPort(),
                cluster.getUsername(),
                cluster.getPassword());
        Map<String, Object> response = new HashMap<>();
        response.put("cluster", cluster);
        response.put("status", status);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody Cluster cluster) throws IOException {
        Cluster registerCluster = clusterService.add(cluster);
        dictionaryService.fetchSystemIndex(registerCluster.getId());
        referenceService.fetchSystemIndex(registerCluster.getId());
        return new ResponseEntity<>(registerCluster, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable String id) {
        Cluster removeCluster = clusterService.remove(UUID.fromString(id));
        return new ResponseEntity<>(removeCluster, HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable String id,
                                  @RequestBody Cluster cluster) throws NotFoundException {
        Cluster editCluster = clusterService.edit(UUID.fromString(id), cluster);
        return new ResponseEntity<>(editCluster, HttpStatus.OK);
    }

}
