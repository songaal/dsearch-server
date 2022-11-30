package com.danawa.dsearch.server.jdbc.service;

import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;
import org.apache.commons.lang.NullArgumentException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface JdbcService {

    void initialize(UUID clusterId) throws IOException;
    List<JdbcInfo> findAll(UUID clusterId) throws IOException;

    boolean create(UUID clusterId, JdbcCreateRequest createRequest) throws IOException, NullArgumentException;

    boolean delete(UUID clusterId, String id) throws IOException, NullArgumentException;
    boolean update(UUID clusterId, String id, JdbcUpdateRequest jdbcRequest) throws IOException;
    String download(UUID clusterId, Map<String, Object> message);

    boolean isConnectable(JdbcUpdateRequest jdbcRequest);
}
