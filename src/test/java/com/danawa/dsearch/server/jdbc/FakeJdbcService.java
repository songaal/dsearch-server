package com.danawa.dsearch.server.jdbc;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class FakeJdbcService extends JdbcService {
    public FakeJdbcService(String jdbcIndex, IndicesService indicesService, ElasticsearchFactory elasticsearchFactory) {
        super(jdbcIndex, indicesService, elasticsearchFactory);
    }

    public boolean connectionTest(JdbcRequest jdbcRequest){
        if(jdbcRequest.getAddress().equals("localhost")){
            return true;
        }else{
            return false;
        }
    }
    public List<Map<String, Object>> getJdbcList(UUID clusterId) throws IOException {
        if(clusterId == null){
            throw new IOException("");
        }else {
            return new ArrayList<>();
        }
    }

    public boolean addJdbcSource(UUID clusterId, JdbcRequest jdbcRequest) throws IOException {
        if(clusterId == null || jdbcRequest == null){
            throw new IOException("");
        }else {
            return true;
        }
    }


    public boolean deleteJdbcSource(UUID clusterId, String id) throws IOException {
        if(clusterId == null || id == null){
            throw new NullArgumentException("");
        } else if (id.equals("")) {
            return false;
        }

        return true;
    }

    public boolean updateJdbcSource(UUID clusterId, String id, JdbcRequest jdbcRequest) throws IOException {
        if(clusterId == null || id == null || jdbcRequest == null){
            throw new NullArgumentException("");
        } else if (id.equals("")) {
            return false;
        }

        return true;
    }

    public String download(UUID clusterId, Map<String, Object> message){
        StringBuffer sb = new StringBuffer();

        if(clusterId == null || message == null){
            return sb.toString();
        }else{
            sb.append("hello world");

            Map<String, Object> jdbc = new HashMap<>();
            jdbc.put("result", false);
            jdbc.put("count", 0);
            jdbc.put("message", "");
            jdbc.put("list", new ArrayList<>());
            message.put("jdbc", jdbc);
        }
        return sb.toString();
    }
}
