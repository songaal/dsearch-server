package com.danawa.fastcatx.server.entity;

import java.io.Serializable;

public class RankingTuningRequest implements Serializable {
    private String text;
    private String index;

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
