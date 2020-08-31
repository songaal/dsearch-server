package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class DictionarySearchRequest implements Serializable {
    private String word;

    public void setWord(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }
}
