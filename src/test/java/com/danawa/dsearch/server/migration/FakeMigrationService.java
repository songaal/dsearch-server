package com.danawa.dsearch.server.migration;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.migration.service.MigrationService;
import com.danawa.dsearch.server.pipeline.service.PipelineService;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class FakeMigrationService extends MigrationService {
    public FakeMigrationService(ElasticsearchFactory elasticsearchFactory, PipelineService pipelineService) {
        super(elasticsearchFactory, pipelineService);
    }

    public Map<String, Object> uploadFile(UUID clusterId, MultipartFile file) {
        Map<String, Object> result = new HashMap<>();
        result.put("result", true);
        result.put("message", "success");

        if(file.isEmpty()){
            result.put("result", false);
            result.put("message", "IOException");
        }


        return result;
    }
}
