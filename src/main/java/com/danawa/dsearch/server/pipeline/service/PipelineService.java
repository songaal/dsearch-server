package com.danawa.dsearch.server.pipeline.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.pipeline.adapter.PipelineAdapter;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.apache.commons.lang.NullArgumentException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;

@Service
public class PipelineService {
    private static Logger logger = LoggerFactory.getLogger(PipelineService.class);

    private PipelineAdapter pipelineAdapter;

    public PipelineService(PipelineAdapter pipelineAdapter) {
        this.pipelineAdapter = pipelineAdapter;
    }
    public String getPipeLineLists(UUID clusterId) throws IOException {
        if(clusterId == null){
            throw new NullArgumentException("");
        }

        return pipelineAdapter.getPipelineLists(clusterId);
    }

    public String testPipeline(UUID clusterId, String name, String body, boolean isDetail) throws IOException {
        String detail = "";
        if(isDetail){
            detail = "?verbose";
        }

        return pipelineAdapter.testPipeline(clusterId, name, detail, body);
    }

    public String addPipeLine(UUID clusterId, String name, String body) throws IOException {
        return pipelineAdapter.addPipeline(clusterId, name, body);
    }

    public String getPipeLine(UUID clusterId, String name) throws IOException {
        return pipelineAdapter.getPipeline(clusterId, name);
    }

    public String deletePipeLine(UUID clusterId, String name) throws IOException {
        return pipelineAdapter.deletePipeline(clusterId, name);
    }

    public String download(UUID clusterId, Map<String, Object> message) throws IOException{
        // 1. 파이프라인 조회
        String pipelines = getPipeLineLists(clusterId);

        // 2. 파이프라인 json 형태로 변경
        StringBuffer sb = new StringBuffer();

        Gson gson = JsonUtils.createCustomGson();
        Map<String, Object> pipelineMap = gson.fromJson(pipelines, Map.class);
        Map<String, Object> result = new HashMap<>();
        List<String> list = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for(String key : pipelineMap.keySet()){
            if(key.startsWith("xpack")){
                keys.add(key);
            }
        }

        for(String key: keys){
            pipelineMap.remove(key);
        }

        for(String key : pipelineMap.keySet()){
            list.add(key);
        }

        result.put("result", true);
        result.put("count", pipelineMap.size());
        result.put("list", list);

        message.put("pipeline", result);
        return gson.toJson(pipelineMap);
    }
}
