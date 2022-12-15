package com.danawa.dsearch.server.jdbc.service;

import com.danawa.dsearch.server.indices.service.IndicesService;
import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.gson.Gson;
import org.apache.commons.lang.NullArgumentException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

@Service
public class JdbcServiceImpl implements JdbcService{
    private static Logger logger = LoggerFactory.getLogger(JdbcServiceImpl.class);
    private String jdbcIndex;
    private final String JDBC_JSON = "jdbc.json";
    private IndicesService indicesService;

    private JdbcRepositoryAdapter jdbcRepositoryAdapter;
    
    public JdbcServiceImpl(@Value("${dsearch.jdbc.setting}") String jdbcIndex,
                           IndicesService indicesService,
                           JdbcRepositoryAdapter jdbcRepositoryAdapter) {
        this.jdbcIndex = jdbcIndex;
        this.indicesService = indicesService;
        this.jdbcRepositoryAdapter = jdbcRepositoryAdapter;
    }

    @Override
    public void initialize(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, jdbcIndex, JDBC_JSON);
    }

    @Override
    public boolean isConnectable(JdbcUpdateRequest jdbcRequest){
        boolean flag = false;

        // 파라미터 체크
        if(jdbcRequest.getUrl() == null
                || jdbcRequest.getDriver() == null
                || jdbcRequest.getPassword() == null
                || jdbcRequest.getUser() == null){
            return flag;
        }

        try{
            String url = jdbcRequest.getUrl();
            Properties properties = new Properties();
            properties.put("user", jdbcRequest.getUser());
            properties.put("password", jdbcRequest.getPassword());

            Class.forName(jdbcRequest.getDriver());

            try(Connection connection = DriverManager.getConnection(url, properties)){
                flag = true;
            }
        }catch (SQLException sqlException){
            logger.error("{}", sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            logger.error("{}", classNotFoundException);
        } catch (Exception e){
            logger.error("{}", e);
        }

        return flag;
    }

    @Override
    public List<JdbcInfo> findAll(UUID clusterId) throws IOException {
        if(clusterId == null) throw new NullArgumentException("clusterId 가 null 입니다.");
        List<JdbcInfo> result = jdbcRepositoryAdapter.findAll(clusterId);
        return result;
    }

    @Override
    public boolean create(UUID clusterId, JdbcCreateRequest createRequest) throws IOException, NullArgumentException {
        if(clusterId == null || createRequest == null){
            throw new NullArgumentException("");
        }

        return jdbcRepositoryAdapter.create(clusterId, createRequest);
    }


    public boolean delete(UUID clusterId, String id) throws IOException, NullArgumentException {
        if(clusterId == null || id == null){
            logger.info("clusterId or id is null");
            throw new NullArgumentException("");
        } else if (id.equals("")) {
            return false;
        }
        return jdbcRepositoryAdapter.delete(clusterId, id);
    }

    @Override
    public boolean update(UUID clusterId, String id, JdbcUpdateRequest jdbcUpdateRequest) throws IOException {
        if(clusterId == null || id == null || jdbcUpdateRequest == null){
            throw new NullArgumentException("");
        } else if (id.equals("")) {
            return false;
        }

        return jdbcRepositoryAdapter.update(clusterId, id, jdbcUpdateRequest);
    }

    @Override
    public String download(UUID clusterId, Map<String, Object> message){
        StringBuffer sb = new StringBuffer();
        Map<String, Object> jdbc = new HashMap<>();
        jdbcRepositoryAdapter.fillJdbcInfoList(clusterId, sb, jdbc);
        message.put("jdbc", jdbc);
        return sb.toString();
    }

}
