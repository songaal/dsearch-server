package com.danawa.dsearch.server.entity;

import java.io.Serializable;

public class DictionaryCompileRequest implements Serializable {
    private String type;
    private String ids;

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setIds(String ids) {
        this.ids = ids;
    }

    public String getIds() {
        return ids;
    }

    @Override
    public String toString() {
        return "\n{\n  type: " + type + ",\n  " +
                "ids: " + ids + "\n}";
    }
}
