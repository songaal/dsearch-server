package com.danawa.dsearch.server.jdbc.adapter;

import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface JdbcAdapter {

    List<JdbcInfo> findAll(UUID cluster) throws IOException;
    boolean create(UUID clusterId, JdbcCreateRequest jdbcCreateRequest) throws IOException;
    boolean delete(UUID clusterId, String docId) throws IOException;
    boolean update(UUID clusterId, String id, JdbcUpdateRequest jdbcRequest) throws IOException;
    void fillJdbcInfoList(UUID clusterId, StringBuffer sb, Map<String, Object> jdbc) ;
}
