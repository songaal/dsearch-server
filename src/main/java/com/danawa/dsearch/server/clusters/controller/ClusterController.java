package com.danawa.dsearch.server.clusters.controller;

import com.danawa.dsearch.server.clusters.service.ClusterRoutingAllocationService;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.entity.ClusterStatusResponse;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.dictionary.service.DictionaryService;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.excpetions.NotFoundException;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
import com.danawa.dsearch.server.reference.service.ReferenceService;
import com.danawa.dsearch.server.templates.service.IndexTemplateService;
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
    private final IndexTemplateService indexTemplateService;
    private final ClusterRoutingAllocationService clusterRoutingAllocationService;

    public ClusterController(@Value("${dsearch.delete}") String deletePrefix,
                             ClusterService clusterService,
                             DictionaryService dictionaryService,
                             ReferenceService referenceService,
                             CollectionService collectionService,
                             IndicesService indicesService, JdbcService jdbcService,
                             IndexTemplateService indexTemplateService,
                             ClusterRoutingAllocationService clusterRoutingAllocationService) {
        this.deletePrefix = deletePrefix;
        this.clusterService = clusterService;
        this.dictionaryService = dictionaryService;
        this.referenceService = referenceService;
        this.collectionService = collectionService;
        this.indicesService = indicesService;
        this.jdbcService = jdbcService;
        this.indexTemplateService = indexTemplateService;
        this.clusterRoutingAllocationService = clusterRoutingAllocationService;
    }

    @GetMapping
    public ResponseEntity<?> findAll(@RequestParam(required = false) boolean isStatus) {
        List<Map<String, Object>> response = new ArrayList<>();
        List<Cluster> clusterList = clusterService.findAll();
        int size = clusterList.size();

        for (int i = 0; i < size; i++) {
            Cluster cluster = clusterList.get(i);
            Map<String, Object> clusterMap = new HashMap<>();

            if (isStatus) {
                ClusterStatusResponse status = clusterService.scanClusterStatus(cluster.getScheme(),
                        cluster.getHost(),
                        cluster.getPort(),
                        cluster.getUsername(),
                        cluster.getPassword());
                clusterMap.put("status", status);
            } else {
                Map<String, Object> statusMap = new HashMap<>();
                statusMap.put("connection", false);
                clusterMap.put("status", statusMap);
            }

            clusterMap.put("cluster", cluster);
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
    public ResponseEntity<?> add(@RequestBody Cluster cluster) throws IOException, NullPointerException {
        Cluster registerCluster = clusterService.add(cluster);
        dictionaryService.fetchSystemIndex(registerCluster.getId());
        referenceService.fetchSystemIndex(registerCluster.getId());
        collectionService.fetchSystemIndex(registerCluster.getId());
        indicesService.fetchSystemIndex(registerCluster.getId());
        jdbcService.fetchSystemIndex(registerCluster.getId()); /* JDBC 인덱스 추가 */
        indexTemplateService.fetchSystemIndex(registerCluster.getId()); /* 인덱스 템플릿 추가 */
        return new ResponseEntity<>(registerCluster, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable String id,
                                    @RequestHeader(value = "client-ip", required = false) String clientIP,
                                    @RequestParam(required = false, defaultValue = "false") String isRemoveData) {
        if ("true".equalsIgnoreCase(isRemoveData)) {
            logger.info("클러스터의 시스템인덱스를 삭제합니다. ==> {}, {}", id, clientIP);
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

    @PutMapping("/check")
    public ResponseEntity<?> check(@RequestHeader(value = "cluster-id") UUID clusterId,
                                   @RequestParam boolean flag) throws IOException {

        clusterRoutingAllocationService.updateClusterAllocation(clusterId, flag ? "none" : "all");

        if(flag){
            // 해당 클러스터의 스케줄 제거
            logger.info("클러스터 점검 시작, clusterId: {}", clusterId);
            collectionService.removeAllSchedule(clusterId);
        }else{
            // 해당 클러스터의 스케줄 재 등록
            logger.info("클러스터 점검 완료, clusterId: {}", clusterId);
            collectionService.registerAllSchedule(clusterId);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
