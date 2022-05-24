package com.danawa.dsearch.server.migration;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.pipeline.PipelineService;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
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

    private final ElasticsearchFactory elasticsearchFactory;
    private final PipelineService pipelineService;

    public MigrationService(ElasticsearchFactory elasticsearchFactory, PipelineService pipelineService) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.pipelineService = pipelineService;
    }

    public Map<String, Object> uploadFile(UUID clusterId, MultipartFile file) {
        Map<String, Object> result = new HashMap<>();
        try(RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)){
            InputStream in = file.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(in);
            BufferedReader br = new BufferedReader(new InputStreamReader(bis));
            Gson gson = JsonUtils.createCustomGson();

            String line = null;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

            Map<String, Object> body = gson.fromJson(sb.toString(), Map.class);
            if(body.get("pipeline") == null && body.get("collection") == null
                    && body.get("jdbc") == null
                    && body.get("templates") == null
                    && body.get("comments") == null){

                result.put("result", false);
                result.put("message", "Uploaded File is not Correct Format");
                logger.info("{} is not correct file uploaded", clusterId);
                return result;
            }

            // 1. 파이프라인
            Map<String, Object> pipelines = (Map<String, Object>) body.get("pipeline");

            if(pipelines != null){
                List<String> list = new ArrayList<String>();
                for(String key : pipelines.keySet()){
                    if(key.startsWith("xpack")) continue;

                    Map<String, Object> pipeline = gson.fromJson(gson.toJson(pipelines.get(key)), Map.class);
                    pipeline.remove("version");
                    String convertBody = gson.toJson(pipeline);
                    logger.info("pipeline: {}, body: {}", key, convertBody);
                    pipelineService.addPipeLine(clusterId, key, convertBody);
                    list.add(key);
                }
                result.put("pipeline", list);
            }else{
                logger.info("clusterId: {}, pipelines is null", clusterId);
                result.put("pipeline", new ArrayList<>());
            }

            // 2. 컬렉션
            List<Object> collection = (List<Object>) body.get("collection");
            if(collection != null) {
                List<String> list = insertIndex(client, collection, gson);
                result.put("collection", list);
            } else {
                logger.info("clusterId: {}, collection is null", clusterId);
                result.put("collection", new ArrayList<>());
            }

            // 3. jdbc
            List<Object> jdbc = (List<Object>) body.get("jdbc");
            if(jdbc != null) {
                List<String> list = insertIndex(client, jdbc, gson);
                result.put("jdbc", list);
            } else {
                logger.info("clusterId: {}, jdbc is null", clusterId);
                result.put("jdbc", new ArrayList<>());
            }

            // 4. 템플릿
            RestClient restClient = client.getLowLevelClient();
            Map<String, Object> templates = (Map<String, Object>) body.get("templates");
            if(templates != null){
                List<String> list = new ArrayList<>();
                for(String key : templates.keySet()){
                    if(key.startsWith(".")){
                        continue;
                    }

                    Map<String, Object> template = (Map<String, Object>) templates.get(key);
                    logger.info("template: {}, body: {}", key, template);
                    Request request = new Request("PUT", "_template/" + key);
                    request.setJsonEntity(gson.toJson(template));
                    Response response = restClient.performRequest(request);
                    logger.info("template: {}, response: {}", key, EntityUtils.toString(response.getEntity()));
                    list.add(key);
                }
                result.put("templates", list);
            }else{
                logger.info("clusterId: {}, templates is null", clusterId);
                result.put("templates", new ArrayList<>());
            }

            // 5. 코멘트
            List<Object> comments = (List<Object>) body.get("comments");
            if(comments != null) {
                List<String> list = insertIndex(client, comments, gson);
                result.put("comments", list);
            } else {
                logger.info("clusterId: {}, comments is null", clusterId);
                result.put("comments", new ArrayList<>());
            }

            client.close();
            br.close();
            bis.close();
            in.close();

            result.put("result", true);
            result.put("message", "success");
        }catch (IOException e){
            logger.info("{}", e.getMessage());
            result.put("result", false);
            result.put("message", e.getMessage());
        }

        return result;
    }


    private List<String> insertIndex(RestHighLevelClient client, List<Object> list, Gson gson) throws IOException {
        List<String> result = new ArrayList<>();
        for(Object item : list){
            String itemBody = gson.toJson(item);
            Map<String, Object> body = gson.fromJson(itemBody, Map.class);
            String index = (String) body.get("_index");
            String id = (String) body.get("_id");
            IndexRequest request = new IndexRequest(index);
            request.id(id);
            request.type("_doc");
            String source = gson.toJson(body.get("_source"));
            logger.info("index: {}, id: {}, source: {}", index, id, source);
            request.source(source, XContentType.JSON);
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            logger.info("index:{}, response: {}", index, response);

            if(index.equals(".dsearch_collection")){
                Map<String,Object> sourceMap = gson.fromJson(source, Map.class);
                result.add((String) sourceMap.get("baseId"));
            }else if(index.equals(".dsearch_jdbc")){
                Map<String,Object> sourceMap = gson.fromJson(source, Map.class);
                String jdbcId = (String) sourceMap.get("id");
                String name = (String) sourceMap.get("name");
                result.add(jdbcId + " [" + name + "]");
            }else if(index.equals(".dsearch_mapping_comments")){
                Map<String,Object> sourceMap = gson.fromJson(source, Map.class);
                String name = (String) sourceMap.get("name");
                result.add(name);
            }
        }
        return result;
    }
}
