package com.danawa.fastcatx.server.entity;

import java.io.Serializable;

public class AutoCompleteUrlRequest  implements Serializable {
    private String url;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
