package com.danawa.dsearch.server.migration.adapter;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.google.gson.Gson;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class MigrationEsAdapter implements MigrationAdapter{
    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    MigrationEsAdapter(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper){
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    public String migrateTemplate(UUID clusterId, String templateName, String template) throws IOException{
        return elasticsearchFactoryHighLevelWrapper.migrateTemplate(clusterId, templateName, template);
    }

    public List<String> migrateIndexDocument(UUID clusterId, List<Object> list, Gson gson) throws IOException {
        List<String> result = new ArrayList<>();

        for (Object item : list) {
            String itemBody = gson.toJson(item);
            Map<String, Object> body = gson.fromJson(itemBody, Map.class);
            String index = (String) body.get("_index");
            String id = (String) body.get("_id");
            String source = gson.toJson(body.get("_source"));

            IndexRequest request = makeIndexRequest(index, id, source);
            IndexResponse response = elasticsearchFactoryHighLevelWrapper.insertDocument(clusterId, request);

            Map<String, Object> sourceMap = gson.fromJson(source, Map.class);
            result.add((String) sourceMap.get("baseId"));
        }
        return result;
    }

    private IndexRequest makeIndexRequest(String index,  String id, String source){
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.type("_doc");
        request.source(source, XContentType.JSON);
        return request;
    }
}
