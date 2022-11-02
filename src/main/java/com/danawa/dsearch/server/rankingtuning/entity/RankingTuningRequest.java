package com.danawa.dsearch.server.rankingtuning.entity;

import java.io.Serializable;

public class RankingTuningRequest implements Serializable {
    private boolean isMultiple;
    private String text;
    private String index;

    public boolean isMultiple() {
        return isMultiple;
    }
    public void setIsMultiple(boolean isMultiple) {
        this.isMultiple = isMultiple ;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return index + " : " + text;
    }
}
