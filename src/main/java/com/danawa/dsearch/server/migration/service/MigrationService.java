package com.danawa.dsearch.server.migration.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.migration.adapter.MigrationAdapter;
import com.danawa.dsearch.server.pipeline.service.PipelineService;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;

@Service
public class MigrationService {
    private static Logger logger = LoggerFactory.getLogger(MigrationService.class);
    private PipelineService pipelineService;
    private MigrationAdapter migrationAdapter;

    public MigrationService(PipelineService pipelineService,
                            MigrationAdapter migrationAdapter) {
        this.pipelineService = pipelineService;
        this.migrationAdapter = migrationAdapter;
    }

    public Map<String, Object> uploadFile(UUID clusterId, MultipartFile file) {
        Map<String, Object> result = new HashMap<>();
        InputStream in = null;
        BufferedInputStream bis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        Gson gson = JsonUtils.createCustomGson();
        StringBuilder sb = new StringBuilder();

        try{
            in = file.getInputStream();
            bis = new BufferedInputStream(in);
            isr = new InputStreamReader(bis);
            br = new BufferedReader(isr);

            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            logger.error("{}", e.getMessage());
            result.put("result", false);
            result.put("message", e.getMessage());
            return result;
        }

        Map<String, Object> body = gson.fromJson(sb.toString(), Map.class);
        if (body.get("pipeline") == null && body.get("collection") == null
                && body.get("jdbc") == null
                && body.get("templates") == null
                && body.get("comments") == null) {

            result.put("result", false);
            result.put("message", "Uploaded File is not Correct Format");
            logger.info("{} is not correct file uploaded", clusterId);
            return result;
        }

        // 1. 파이프라인
        try{
            List<String> pipelineList = migratePipeline(clusterId, body, gson);
            result.put("pipeline", pipelineList);
        }catch (IOException e){
            logger.error("{}", e);
        }

        // 2. 컬렉션
        try{
            List<String> collectionList = migrateCollection(clusterId, body, gson);
            result.put("collection", collectionList);
        }catch (IOException e){
            logger.error("{}", e);
        }


        // 3. jdbc
        try{
            List<String> jdbcList = migrateJdbc(clusterId, body, gson);
            result.put("jdbc", jdbcList);
        }catch (IOException e){
            logger.error("{}", e);
        }

        // 4. 템플릿
        try{
            List<String> templatesList = migrateTemplates(clusterId, body, gson);
            result.put("templates", templatesList);
        }catch (IOException e){
            logger.error("{}", e);
        }

        // 5. 코멘트
        try{
            List<String> comments = migrateComments(clusterId, body, gson);
            result.put("comments", comments);
        }catch (IOException e){
            logger.error("{}", e);
        }

        result.put("result", true);
        result.put("message", "success");

        if (in != null) {
            try {
                in.close();
            } catch (IOException ignore) {
            }
        }
        if (bis != null) {
            try {
                bis.close();
            } catch (IOException ignore) {
            }
        }
        if (isr != null) {
            try {
                isr.close();
            } catch (IOException ignore) {
            }
        }
        if (br != null) {
            try {
                br.close();
            } catch (IOException ignore) {

            }
        }

        return result;
    }

    private List<String> migratePipeline(UUID clusterId, Map<String, Object> body, Gson gson) throws IOException {
        Map<String, Object> pipelines = (Map<String, Object>) body.get("pipeline");

        if (pipelines != null) {
            List<String> list = new ArrayList<String>();
            for (String key : pipelines.keySet()) {
                if (key.startsWith("xpack")) continue;

                Map<String, Object> pipeline = gson.fromJson(gson.toJson(pipelines.get(key)), Map.class);
                pipeline.remove("version");
                String convertBody = gson.toJson(pipeline);
                logger.info("pipeline: {}, body: {}", key, convertBody);
                pipelineService.addPipeLine(clusterId, key, convertBody);
                list.add(key);
            }
            return list;
        } else {
            logger.info("clusterId: {}, pipelines is null", clusterId);
            return new ArrayList<>();
        }
    }

    private List<String> migrateCollection(UUID clusterId, Map<String, Object> body, Gson gson) throws IOException {
        List<Object> collection = (List<Object>) body.get("collection");
        if (collection != null) {
            List<String> list = migrationAdapter.migrateIndexDocument(clusterId, collection, gson);
            return list;
        } else {
            logger.info("clusterId: {}, collection is null", clusterId);
            return new ArrayList<>();
        }
    }

    private List<String> migrateJdbc(UUID clusterId, Map<String, Object> body, Gson gson) throws IOException {
        List<Object> jdbc = (List<Object>) body.get("jdbc");
        if (jdbc != null) {
            List<String> list = migrationAdapter.migrateIndexDocument(clusterId, jdbc, gson);
            return list;
        } else {
            logger.info("clusterId: {}, jdbc is null", clusterId);
            return new ArrayList<>();
        }
    }

    private List<String> migrateTemplates(UUID clusterId, Map<String, Object> body, Gson gson) throws IOException {
        Map<String, Object> templates = (Map<String, Object>) body.get("templates");
        if (templates != null) {
            List<String> list = new ArrayList<>();
            for (String name : templates.keySet()) {
                if (name.startsWith(".")) {
                    continue;
                }

                Map<String, Object> template = (Map<String, Object>) templates.get(name);
                String templateString = gson.toJson(template);
                migrationAdapter.migrateTemplate(clusterId, name, templateString);
                logger.info("template: {}, body: {}", name, template);
                list.add(name);
            }
            return list;
        } else {
            logger.info("clusterId: {}, templates is null", clusterId);
        }
        return new ArrayList<>();
    }

    private List<String> migrateComments(UUID clusterId, Map<String, Object> body, Gson gson) throws IOException {
        List<Object> comments = (List<Object>) body.get("comments");
        if (comments != null) {
            List<String> list = migrationAdapter.migrateIndexDocument(clusterId, comments, gson);
            return list;
        } else {
            logger.info("clusterId: {}, comments is null", clusterId);
            return new ArrayList<>();
        }
    }


}
