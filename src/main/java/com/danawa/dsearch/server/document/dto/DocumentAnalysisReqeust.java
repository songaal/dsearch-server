package com.danawa.dsearch.server.document.dto;

import com.danawa.dsearch.server.document.entity.SearchQuery;

import java.io.Serializable;

public class DocumentAnalysisReqeust implements Serializable {
    private boolean viewDetails;
    private SearchQuery searchQuery;

    public SearchQuery getSearchQuery() {
        return searchQuery;
    }

    public void setSearchQuery(SearchQuery searchQuery) {
        this.searchQuery = searchQuery;
    }

    public boolean isViewDetails() {
        return viewDetails;
    }

    public void setViewDetails(boolean viewDetails) {
        this.viewDetails = viewDetails;
    }
}
