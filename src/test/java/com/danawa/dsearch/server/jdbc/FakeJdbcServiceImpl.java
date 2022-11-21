package com.danawa.dsearch.server.jdbc;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import com.danawa.dsearch.server.jdbc.service.JdbcServiceImpl;
import org.apache.commons.lang.NullArgumentException;

import java.io.IOException;
import java.util.*;

public class FakeJdbcServiceImpl extends JdbcServiceImpl {
    public FakeJdbcServiceImpl(String jdbcIndex, IndicesService indicesService, ElasticsearchFactory elasticsearchFactory) {
        super(jdbcIndex, indicesService, elasticsearchFactory);
    }

    public boolean isConnectable(JdbcRequest jdbcRequest){
        // 파라미터 체크
        if(jdbcRequest.getUrl() == null
                || jdbcRequest.getDriver() == null
                || jdbcRequest.getPassword() == null
                || jdbcRequest.getUser() == null){
            return false;
        }

        return true;
    }

    public boolean create(UUID clusterId, JdbcRequest jdbcRequest) throws IOException {
        if(clusterId == null || jdbcRequest == null){
            throw new NullArgumentException("");
        }else {
            return true;
        }
    }


    public boolean delete(UUID clusterId, String id) throws IOException {
        if(clusterId == null || id == null){
            throw new NullArgumentException("");
        } else if (id.equals("")) {
            return false;
        }

        return true;
    }

    public boolean update(UUID clusterId, String id, JdbcRequest jdbcRequest) throws IOException {
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
