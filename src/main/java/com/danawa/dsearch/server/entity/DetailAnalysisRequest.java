package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class DetailAnalysisRequest implements Serializable {
    private String text;
    private String plugin;
    private String useForQuery;

    public String getText(){
        return this.text;
    }

    public String getPlugin(){
        return this.plugin;
    }

    public void setText(String text){
        this.text= text;
    }

    public void setPlugin(String plugin){
        this.plugin = plugin;
    }

    public void setUseForQuery(String useForQuery) {
        this.useForQuery = useForQuery;
    }

    public String getUseForQuery() {
        return this.useForQuery;
    }
}
