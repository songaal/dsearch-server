package com.danawa.dsearch.server.controller;


import com.danawa.dsearch.server.entity.Cluster;
import com.danawa.dsearch.server.services.*;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
    private ReferenceService referenceService;
    private MigrationService migrationService;

    public MigrationController(
            ClusterService clusterService,
                                PipelineService pipelineService,
                               ReferenceService referenceService,
                               CollectionService collectionService,
                               JdbcService jdbcService,
                               IndexTemplateService indexTemplateService,
            MigrationService migrationService) {
        this.clusterService = clusterService;
        this.pipelineService = pipelineService;
        this.referenceService = referenceService;
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
