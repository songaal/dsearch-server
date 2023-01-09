package com.danawa.dsearch.server.migration.adapter;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public interface MigrationAdapter {
    List<String> migrateIndexDocument(UUID clusterId, List<Object> list, Gson gson) throws IOException;

    String migrateTemplate(UUID clusterId, String templateName, String template) throws IOException;
}
