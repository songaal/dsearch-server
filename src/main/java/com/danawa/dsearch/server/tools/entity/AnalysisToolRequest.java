package com.danawa.dsearch.server.tools.entity;

public class AnalysisToolRequest {
    private String plugin;
    private String text;
    private String useForQuery;
    private String analyzer;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUseForQuery() {
        return useForQuery;
    }

    public String getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    public String getPlugin() {
        return plugin;
    }

    public void setPlugin(String plugin) {
        this.plugin = plugin;
    }
}
