package com.danawa.dsearch.server.jdbc.adapter;

import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface JdbcAdapter {

    List<JdbcInfo> findAll(UUID cluster, String index) throws IOException;
    boolean create(UUID clusterId, String index, JdbcInfo jdbcInfo) throws IOException;
    boolean delete(UUID clusterId, String index, String docId) throws IOException;
    boolean update(UUID clusterId, String index, String id, JdbcUpdateRequest jdbcRequest) throws IOException;
    Map<String, Object> getJdbcInfoList(UUID clusterId, String index, StringBuffer sb);
}
