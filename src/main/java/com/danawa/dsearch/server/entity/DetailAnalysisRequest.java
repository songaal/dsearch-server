package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class DetailAnalysisRequest implements Serializable {
    private String text;
    private String plugin;

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
}
