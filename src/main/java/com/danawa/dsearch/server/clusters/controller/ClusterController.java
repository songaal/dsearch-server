package com.danawa.dsearch.server.clusters.controller;

import com.danawa.dsearch.server.clusters.service.ClusterRoutingAllocationService;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.dto.ClusterStatusResponse;
import com.danawa.dsearch.server.collections.service.history.HistoryService;
import com.danawa.dsearch.server.collections.service.schedule.IndexingScheduler;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.collections.service.status.StatusService;
import com.danawa.dsearch.server.dictionary.service.DictionaryService;
import com.danawa.dsearch.server.collections.entity.Collection;
import com.danawa.dsearch.server.documentAnalysis.service.SearchQueryService;
import com.danawa.dsearch.server.excpetions.NotFoundException;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
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
    private final CollectionService collectionService;
    private final JdbcService jdbcService;
    private final IndicesService indicesService;

    private final StatusService indexStatusService;
    private final HistoryService historyService;

    private final IndexTemplateService indexTemplateService;
    private final ClusterRoutingAllocationService clusterRoutingAllocationService;
    private final IndexingScheduler indexingScheduler;

    private SearchQueryService searchQueryService;

    public ClusterController(@Value("${dsearch.delete}") String deletePrefix,
                             ClusterService clusterService,
                             DictionaryService dictionaryService,
                             CollectionService collectionService,
                             IndicesService indicesService,
                             JdbcService jdbcService,
                             IndexTemplateService indexTemplateService,
                             ClusterRoutingAllocationService clusterRoutingAllocationService,
                             IndexingScheduler indexingScheduler,
                             StatusService indexStatusService,
                             HistoryService historyService,
                             SearchQueryService searchQueryService) {
        this.deletePrefix = deletePrefix;
        this.clusterService = clusterService;
        this.dictionaryService = dictionaryService;
        this.collectionService = collectionService;
        this.indicesService = indicesService;
        this.jdbcService = jdbcService;
        this.indexTemplateService = indexTemplateService;
        this.clusterRoutingAllocationService = clusterRoutingAllocationService;
        this.indexingScheduler = indexingScheduler;
        this.indexStatusService = indexStatusService;
        this.historyService = historyService;
        this.searchQueryService = searchQueryService;
    }

    @GetMapping
    public ResponseEntity<?> getClusterInfoAll(@RequestParam(required = false) boolean isStatus) {
        List<Map<String, Object>> response = new ArrayList<>();
        List<Cluster> clusterList = clusterService.findAll();

        for (Cluster cluster: clusterList) {
            Map<String, Object> clusterMap = getClusterInfo(cluster, isStatus);
            response.add(clusterMap);
        }

        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private Map<String, Object> getClusterInfo(Cluster cluster, boolean isStatus){
        Map<String, Object> clusterInfo = new HashMap<>();
        ClusterStatusResponse status;

        if (isStatus) {
            status = clusterService.scanClusterStatus(cluster.getScheme(),
                    cluster.getHost(),
                    cluster.getPort(),
                    cluster.getUsername(),
                    cluster.getPassword());
        } else {
            status = new ClusterStatusResponse(false);
        }

        clusterInfo.put("status", status);
        clusterInfo.put("cluster", cluster);
        return clusterInfo;
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getClusterInfoOne(@PathVariable String id) {
        Cluster cluster = clusterService.findById(UUID.fromString(id));
        Map<String, Object> response = getClusterInfo(cluster, true);
//        ClusterStatusResponse status = clusterService.scanClusterStatus(cluster.getScheme(),
//                cluster.getHost(),
//                cluster.getPort(),
//                cluster.getUsername(),
//                cluster.getPassword());
//
//        Map<String, Object> response = new HashMap<>();
//        response.put("cluster", cluster);
//        response.put("status", status);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody Cluster cluster) throws IOException, NullPointerException {
        Cluster registerCluster = clusterService.add(cluster);
        dictionaryService.initialize(registerCluster.getId());
        indexTemplateService.initialize(registerCluster.getId()); /* 인덱스 템플릿 추가 */
        collectionService.initialize(registerCluster.getId());
        jdbcService.initialize(registerCluster.getId()); /* JDBC 인덱스 추가 */
        historyService.initialize(registerCluster.getId());
        indexStatusService.initialize(registerCluster.getId());
        searchQueryService.initialize(registerCluster.getId());
        indexingScheduler.init(); /* 이전에 등록되어 있던 클러스터라면 스케쥴 재 등록 */
        return new ResponseEntity<>(registerCluster, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable String id,
                                    @RequestHeader(value = "client-ip", required = false) String clientIP,
                                    @RequestParam(required = false, defaultValue = "false") String isRemoveData) {
        if ("true".equalsIgnoreCase(isRemoveData)) {
            logger.info("클러스터의 시스템 인덱스를 삭제합니다. ==> {}, {}", id, clientIP);
            removeSystemIndex(id);
        }
        Cluster removeCluster = clusterService.remove(UUID.fromString(id));
        return new ResponseEntity<>(removeCluster, HttpStatus.OK);
    }

    private void removeSystemIndex(String id){
        try {
            UUID clusterId = UUID.fromString(id);

            List<Collection> collectionList = collectionService.findAll(clusterId);
            for (Collection collection: collectionList) {
                try {
                    if (collection.isScheduled()) {
                        collection.setScheduled(false);
                        collectionService.updateSchedule(clusterId, collection.getId(), collection);
                    }
                } catch (Exception e){
                    logger.error("", e);
                }
            }
            indicesService.delete(clusterId, deletePrefix);
        } catch (Exception e) {
            logger.error("dsearch system index remove fail. {}", e.getMessage());
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable String id,
                                  @RequestBody Cluster cluster) throws NotFoundException {
        Cluster editCluster = clusterService.edit(UUID.fromString(id), cluster);
        return new ResponseEntity<>(editCluster, HttpStatus.OK);
    }

    @PutMapping("/check")
    public ResponseEntity<?> check(@RequestHeader(value = "cluster-id") UUID clusterId,
                                   @RequestParam boolean flag) {

        // 클러스터의 샤드 재분배 설정 변경
        // none : 샤드 재분배 안함
        // all : 샤드 재분배 함
        clusterRoutingAllocationService.updateClusterAllocation(clusterId, flag);

        if(flag){
            // 해당 클러스터의 스케줄 제거
            logger.info("클러스터 점검 시작, clusterId: {}", clusterId);
            indexingScheduler.unregisterAll(clusterId);
        }else{
            // 해당 클러스터의 스케줄 재 등록
            logger.info("클러스터 점검 완료, clusterId: {}", clusterId);
            indexingScheduler.registerAll(clusterId);
        }
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
