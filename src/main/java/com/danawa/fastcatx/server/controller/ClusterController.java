package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.Cluster;
import com.danawa.fastcatx.server.services.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/clusters")
public class ClusterController {
    private static Logger logger = LoggerFactory.getLogger(ClusterController.class);

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping
    public ResponseEntity<?> findAll() {
        List<Cluster> registerClusterList = clusterService.findAll();
        return new ResponseEntity<>(registerClusterList, HttpStatus.OK);
    }

    @GetMapping("{id}")
    public ResponseEntity<?> find(@PathVariable Long id) {
        Cluster registerCluster = clusterService.find(id);
        return new ResponseEntity<>(registerCluster, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody Cluster cluster) {
        Cluster registerCluster = clusterService.add(cluster);
        return new ResponseEntity<>(registerCluster, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable Long id) {
        Cluster removeCluster = clusterService.remove(id);
        return new ResponseEntity<>(removeCluster, HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable Long id,
                                  @RequestBody Cluster cluster) {
        Cluster editCluster = clusterService.edit(id, cluster);
        return new ResponseEntity<>(editCluster, HttpStatus.OK);
    }

}
