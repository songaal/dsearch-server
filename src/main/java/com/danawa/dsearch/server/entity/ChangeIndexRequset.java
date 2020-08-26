package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class ChangeIndexRequset implements Serializable {
    private String aliases;
    private String currentIndex;
    private String changeIndex;

    public String getAliases() {
        return aliases;
    }

    public String getChangeIndex() {
        return changeIndex;
    }

    public String getCurrentIndex() {
        return currentIndex;
    }

    public void setAliases(String aliases) {
        this.aliases = aliases;
    }

    public void setChangeIndex(String changeIndex) {
        this.changeIndex = changeIndex;
    }

    public void setCurrentIndex(String currentIndex) {
        this.currentIndex = currentIndex;
    }
}
