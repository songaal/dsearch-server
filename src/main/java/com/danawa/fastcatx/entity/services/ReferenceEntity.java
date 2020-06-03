package com.danawa.fastcatx.entity.services;

import java.io.Serializable;
import java.util.List;

public class ReferenceEntity implements Serializable {
    private String name;
    private String query;
    private String title;
    private String thumbnails;
    private List<Field> fields;
    private List<Field> aggs;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getThumbnails() {
        return thumbnails;
    }

    public void setThumbnails(String thumbnails) {
        this.thumbnails = thumbnails;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<Field> getAggs() {
        return aggs;
    }

    public void setAggs(List<Field> aggs) {
        this.aggs = aggs;
    }

    public static class Field {
        private String label;
        private String value;

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}