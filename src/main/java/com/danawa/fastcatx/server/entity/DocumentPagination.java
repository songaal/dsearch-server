package com.danawa.fastcatx.server.entity;

import org.elasticsearch.search.SearchHits;

import java.io.Serializable;

public class DocumentPagination implements Serializable {
    private SearchHits hits;
    private long totalCount;
    private int from;
    private int size;


    public SearchHits getHits() {
        return hits;
    }

    public void setHits(SearchHits hits) {
        this.hits = hits;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
}
