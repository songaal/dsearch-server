package com.danawa.dsearch.server.jdbc.service;

import com.danawa.dsearch.server.collections.entity.IndexingStatus;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import org.apache.commons.lang.NullArgumentException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface JdbcService {

    void initialize(UUID clusterId) throws IOException;
    List<Map<String, Object>> findAll(UUID clusterId) throws IOException;

    boolean create(UUID clusterId, JdbcRequest jdbcRequest) throws IOException, NullArgumentException ;
    boolean delete(UUID clusterId, String id) throws IOException, NullArgumentException;
    boolean update(UUID clusterId, String id, JdbcRequest jdbcRequest) throws IOException;
    String download(UUID clusterId, Map<String, Object> message);

    boolean isConnectable(JdbcRequest jdbcRequest);
}
