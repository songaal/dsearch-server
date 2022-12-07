package com.danawa.dsearch.server.pipeline;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.pipeline.service.PipelineService;
import org.apache.commons.lang.NullArgumentException;

import java.io.IOException;
import java.util.UUID;

public class FakePipelineService extends PipelineService {
    public FakePipelineService(ElasticsearchFactory elasticsearchFactory) {
        super(elasticsearchFactory);
    }

    public String getPipeLineLists(UUID clusterId) throws IOException {
        if(clusterId == null){
            throw new NullArgumentException("");
        }
        return "pipelines";
    }

    public String getPipeLine(UUID clusterId, String name) throws IOException {
        if(clusterId == null || name == null || name.equals("")){
            throw new NullArgumentException("");
        }
        return "pipeline";
    }

    public String addPipeLine(UUID clusterId, String name, String body) throws IOException {
        if(clusterId == null || name == null || name.equals("") || body == null || body.equals("")){
            throw new NullArgumentException("");
        }
        return "added";
    }

    public String deletePipeLine(UUID clusterId, String name) throws IOException {
        if(clusterId == null || name == null || name.equals("")){
            throw new NullArgumentException("");
        }
        return "delete";
    }

    public String testPipeline(UUID clusterId, String name, String body, boolean isDetail) throws IOException {
        if(clusterId == null || name == null || name.equals("") || body == null || body.equals("")){
            throw new NullArgumentException("");
        }
        return "test";
    }

}
