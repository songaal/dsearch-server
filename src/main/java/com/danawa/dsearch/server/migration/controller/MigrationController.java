package com.danawa.dsearch.server.migration.controller;


import com.danawa.dsearch.server.clusters.service.ClusterService;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
import com.danawa.dsearch.server.migration.service.MigrationService;
import com.danawa.dsearch.server.pipeline.service.PipelineService;
import com.danawa.dsearch.server.templates.service.IndexTemplateService;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;

@RestController
@RequestMapping("/migration")
public class MigrationController {
    private static Logger logger = LoggerFactory.getLogger(MigrationController.class);
    private ClusterService clusterService;
    private CollectionService collectionService;
    private JdbcService jdbcService;
    private PipelineService pipelineService;
    private IndexTemplateService indexTemplateService ;
    private MigrationService migrationService;

    public MigrationController(
            ClusterService clusterService,
                                PipelineService pipelineService,
                               CollectionService collectionService,
                               JdbcService jdbcService,
                               IndexTemplateService indexTemplateService,
            MigrationService migrationService) {
        this.clusterService = clusterService;
        this.pipelineService = pipelineService;
        this.collectionService = collectionService;
        this.jdbcService = jdbcService;
        this.indexTemplateService = indexTemplateService;
        this.migrationService = migrationService;
    }

    @PostMapping("/download")
    public ResponseEntity<?> download(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @RequestParam("pipeline") boolean pipeline,
                                      @RequestParam("collection") boolean collection,
                                      @RequestParam("jdbc") boolean jdbc,
                                      @RequestParam("templates") boolean templates) throws IOException {

        logger.info("clusterId: {}, pipeline: {}, collections: {}, jdbc: {}, templates: {}, comments: {}", clusterId, pipeline, collection, jdbc, templates);
        Cluster cluster = clusterService.find(clusterId);

        Map<String, Object> message = new HashMap<>();
        HttpHeaders headers = new HttpHeaders();
        StringBuilder sb = new StringBuilder();

        sb.append("{\n");

        boolean firstFlag = false;
        if(pipeline){
            sb.append("\"pipeline\": " + pipelineService.download(clusterId, message) + "\n");
            firstFlag = true;
        }

        if(collection){
            if(firstFlag) { sb.append(",\n"); }
            sb.append("\"collection\": [" + collectionService.download(clusterId, message) + "]\n");
            firstFlag = true;
        }

        if(jdbc){
            if(firstFlag) {sb.append(",\n"); }
            sb.append("\"jdbc\": [" + jdbcService.download(clusterId, message) + "]\n");
            firstFlag = true;
        }

        if(templates){
            if(firstFlag) sb.append(",\n");
            sb.append("\"templates\": " + indexTemplateService.download(clusterId, message));
            sb.append(",\n");
            sb.append("\"comments\": [" + indexTemplateService.commentDownload(clusterId, message) + "]");
            firstFlag = true;
        }

        Gson gson = JsonUtils.createCustomGson();
        if(firstFlag) {sb.append(",\n"); }
        sb.append("\"result\": " + gson.toJson(message));
        sb.append("\n}");

        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=dsearch-"+ cluster.getName()+ "-backup.txt");
        return new ResponseEntity<>(sb.toString(), headers, HttpStatus.OK);
    }

    @PostMapping("/upload")
    public ResponseEntity<?> upload(@RequestHeader(value = "cluster-id") UUID clusterId,
                                    @RequestParam("filename") MultipartFile file){
        return new ResponseEntity<>(migrationService.uploadFile(clusterId, file), HttpStatus.OK);
    }

}
