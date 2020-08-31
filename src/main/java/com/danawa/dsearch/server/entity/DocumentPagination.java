package com.danawa.dsearch.server.entity;

import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DocumentPagination implements Serializable {
    private Set<String> fields;
    private SearchHit[] hits;
    private Map<String, Aggregation> aggregations;
    private long totalCount;
    private long lastPageNum;
    private long pageNum;
    private long rowSize;
    private Map<String, Map<String, List<AnalyzeResponse.AnalyzeToken>>> analyzeDocumentTermMap;

    public Map<String, Map<String, List<AnalyzeResponse.AnalyzeToken>>> getAnalyzeDocumentTermMap() {
        return analyzeDocumentTermMap;
    }

    public void setAnalyzeDocumentTermMap(Map<String, Map<String, List<AnalyzeResponse.AnalyzeToken>>> analyzeDocumentTermMap) {
        this.analyzeDocumentTermMap = analyzeDocumentTermMap;
    }

    public Set<String> getFields() {
        return fields;
    }

    public void setFields(Set<String> fields) {
        this.fields = fields;
    }

    public SearchHit[] getHits() {
        return hits;
    }

    public void setHits(SearchHit[] hits) {
        this.hits = hits;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getLastPageNum() {
        return lastPageNum;
    }

    public void setLastPageNum(long lastPageNum) {
        this.lastPageNum = lastPageNum;
    }

    public long getPageNum() {
        return pageNum;
    }

    public void setPageNum(long pageNum) {
        this.pageNum = pageNum;
    }

    public long getRowSize() {
        return rowSize;
    }

    public void setRowSize(long rowSize) {
        this.rowSize = rowSize;
    }

    public Map<String, Aggregation> getAggregations() {
        return aggregations;
    }

    public void setAggregations(Map<String, Aggregation> aggregations) {
        this.aggregations = aggregations;
    }
}
