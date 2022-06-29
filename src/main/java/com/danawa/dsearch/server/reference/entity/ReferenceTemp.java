package com.danawa.dsearch.server.reference.entity;

import java.io.Serializable;
import java.util.List;

public class ReferenceTemp implements Serializable {
    private String id;
    private String name;
    private String indices;
    private String query;
    private String title;
    private String clickUrl;
    private String thumbnails;
    private Integer order;
    private List<Field> fields;
    private List<Field> aggs;

    public ReferenceTemp() {}

    public ReferenceTemp(String id, String name, String indices, String query, String title, String clickUrl, String thumbnails, Integer order, List<Field> fields, List<Field> aggs) {
        this.id = id;
        this.name = name;
        this.indices = indices;
        this.query = query;
        this.title = title;
        this.clickUrl = clickUrl;
        this.thumbnails = thumbnails;
        this.order = order;
        this.fields = fields;
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getIndices() {
        return indices;
    }

    public void setIndices(String indices) {
        this.indices = indices;
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

    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }
    public void setOrder(String order) {
        this.order = order == null ? 0 : Integer.parseInt(order);
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

    public String getClickUrl() {
        return clickUrl;
    }

    public void setClickUrl(String clickUrl) {
        this.clickUrl = clickUrl;
    }
}