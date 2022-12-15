package com.danawa.dsearch.server.document.controller;

import com.danawa.dsearch.server.document.dto.SearchQueryCreateRequest;
import com.danawa.dsearch.server.document.dto.SearchQueryUpdateRequest;
import com.danawa.dsearch.server.document.entity.SearchQuery;
import com.danawa.dsearch.server.document.service.DocumentAnalysisService;
import com.danawa.dsearch.server.document.service.SearchQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/document/searchQuery")
public class SearchQueryController {
    private static Logger logger = LoggerFactory.getLogger(SearchQueryController.class);
    private final SearchQueryService searchQueryService;

    public SearchQueryController(SearchQueryService searchQueryService) {
        this.searchQueryService = searchQueryService;
    }

    @GetMapping
    public ResponseEntity<?> getSearchQueryList(@RequestHeader(value = "cluster-id") UUID clusterId) {
        Map<String, Object> response = new HashMap<>();
        List<SearchQuery> result = searchQueryService.getSearchQueryList(clusterId);
        response.put("result", result);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> createSearchQuery(@RequestHeader(value = "cluster-id") UUID clusterId,
                                             @RequestBody SearchQueryCreateRequest searchQueryCreateRequest) {
        Map<String, Object> response = new HashMap<>();
        SearchQuery result = searchQueryService.createSearchQuery(clusterId, searchQueryCreateRequest);
        response.put("result", result);
        response.put("id", result.getId());
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PutMapping
    public ResponseEntity<?> updateSearchQuery(@RequestHeader(value = "cluster-id") UUID clusterId,
                                               @RequestBody SearchQueryUpdateRequest searchQueryUpdateRequest) {
        Map<String, Object> response = new HashMap<>();
        SearchQuery result = searchQueryService.updateSearchQuery(clusterId, searchQueryUpdateRequest);
        response.put("result", result);
        response.put("id", result.getId());

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @DeleteMapping
    public ResponseEntity<?> deleteSearchQuery(@RequestHeader(value = "cluster-id") UUID clusterId,
                                               @RequestParam("id") String id) {
        searchQueryService.deleteSearchQuery(clusterId, id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
