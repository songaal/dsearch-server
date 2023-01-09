package com.danawa.dsearch.server.tools.adapter;

import java.io.IOException;
import java.util.UUID;

public interface ToolsAdapter {

    String getPlugins(UUID clusterId) throws IOException;

    String analysisTextUsingPlugin(UUID clusterId, String index, String pluginName, boolean useForQuery, String text) throws IOException;
}
