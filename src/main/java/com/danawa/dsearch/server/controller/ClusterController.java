package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.Cluster;
import com.danawa.dsearch.server.entity.ClusterStatusResponse;
import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.excpetions.NotFoundException;
import com.danawa.dsearch.server.services.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;

@RestController
@RequestMapping("/clusters")
public class ClusterController {
    private static Logger logger = LoggerFactory.getLogger(ClusterController.class);

    private final String deletePrefix;

    private final ClusterService clusterService;
    private final DictionaryService dictionaryService;
    private final ReferenceService referenceService;
    private final CollectionService collectionService;
    private final JdbcService jdbcService;
    private final IndicesService indicesService;

    public ClusterController(@Value("${dsearch.delete}") String deletePrefix,
                             ClusterService clusterService,
                             DictionaryService dictionaryService,
                             ReferenceService referenceService,
                             CollectionService collectionService,
                             IndicesService indicesService, JdbcService jdbcService) {
        this.deletePrefix = deletePrefix;
        this.clusterService = clusterService;
        this.dictionaryService = dictionaryService;
        this.referenceService = referenceService;
        this.collectionService = collectionService;
        this.indicesService = indicesService;
        this.jdbcService = jdbcService;
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
        collectionService.fetchSystemIndex(registerCluster.getId());
        indicesService.fetchSystemIndex(registerCluster.getId());
        jdbcService.fetchSystemIndex(registerCluster.getId()); /* JDBC 인덱스 추가 */
        return new ResponseEntity<>(registerCluster, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable String id, @RequestParam(required = false, defaultValue = "false") String isRemoveData) throws IOException {
        if ("true".equalsIgnoreCase(isRemoveData)) {
            logger.info("클러스터의 시스템인덱스를 삭제합니다., {}", id);
            try {
                UUID clusterId = UUID.fromString(id);

                List<Collection> collectionList = collectionService.findAll(clusterId);
                for (int i = 0; i < collectionList.size(); i++) {
                    try {
                        Collection collection = collectionList.get(i);
                        if (collection.isScheduled()) {
                            collection.setScheduled(false);
                            collectionService.editSchedule(clusterId, collection.getId(), collection);
                        }
                    } catch (Exception e){
                        logger.error("", e);
                    }
                }
                indicesService.delete(clusterId, deletePrefix);
            } catch (Exception e) {
                logger.warn("dsearch system index remove fail. {}", e.getMessage());
            }
        }
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
