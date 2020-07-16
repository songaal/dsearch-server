package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import com.danawa.fastcatx.server.entity.JdbcRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;


@Service
public class JdbcService {

    private static Logger logger = LoggerFactory.getLogger(JdbcService.class);

    private String jdbcIndex;
    private final String JDBC_JSON = "jdbc.json";
    private final ElasticsearchFactory elasticsearchFactory;
    private IndicesService indicesService;

    public JdbcService(@Value("${fastcatx.jdbc.setting}") String jdbcIndex, IndicesService indicesService, ElasticsearchFactory elasticsearchFactory) {
        this.jdbcIndex = jdbcIndex;
        this.indicesService = indicesService;
        this.elasticsearchFactory = elasticsearchFactory;
    }

    public void fetchSystemIndex(UUID clusterId) throws IOException {
        indicesService.createSystemIndex(clusterId, jdbcIndex, JDBC_JSON);
    }

    public boolean connectionTest(JdbcRequest jdbcRequest){
        boolean flag = false;

        try{
            String url = jdbcRequest.getUrl();
            Class.forName(jdbcRequest.getDriver());
            Connection connection = null;
            connection = DriverManager.getConnection(url, jdbcRequest.getUser(), jdbcRequest.getPassword());
            connection.close();
            flag = true;
        }catch (SQLException sqlException){
            logger.debug("", sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            logger.debug("", classNotFoundException);
        } catch (Exception e){
            logger.debug("", e);
        }

        return flag;
    }
}
