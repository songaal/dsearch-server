package com.danawa.dsearch.server.pipeline.adapter;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Service
public class PipelineEsAdapter implements PipelineAdapter{

    private ElasticsearchFactoryHighLevelWrapper esHighLevelWrapper;

    PipelineEsAdapter(ElasticsearchFactoryHighLevelWrapper esHighLevelWrapper){
        this.esHighLevelWrapper = esHighLevelWrapper;
    }

    public String getPipelineLists(UUID clusterId) throws IOException {
        return esHighLevelWrapper.getPipelineLists(clusterId);
    }

    public String testPipeline(UUID clusterId, String pipelineName, String detail, String body) throws IOException {
        return esHighLevelWrapper.testPipeline(clusterId, pipelineName, detail, body);
    }

    @Override
    public String addPipeline(UUID clusterId, String name, String body) throws IOException {
        return esHighLevelWrapper.addPipeline(clusterId, name, body);
    }

    @Override
    public String deletePipeline(UUID clusterId, String name) throws IOException {
        return esHighLevelWrapper.deletePipeline(clusterId, name);
    }

    @Override
    public String getPipeline(UUID clusterId, String name) throws IOException {
        return esHighLevelWrapper.getPipeline(clusterId, name);
    }
}
