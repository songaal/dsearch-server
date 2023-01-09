package com.danawa.dsearch.server.pipeline.adapter;

import java.io.IOException;
import java.util.UUID;

public interface PipelineAdapter {

    String getPipelineLists(UUID clusterId) throws IOException;

    String testPipeline(UUID clusterId, String pipelineName, String detail, String body) throws IOException;

    String addPipeline(UUID clusterId, String name, String body) throws IOException;
    String deletePipeline(UUID clusterId, String name) throws IOException;
    String getPipeline(UUID clusterId, String name) throws IOException;
}
