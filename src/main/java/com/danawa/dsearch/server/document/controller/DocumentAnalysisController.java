package com.danawa.dsearch.server.document.controller;

import com.danawa.dsearch.server.document.dto.DocumentAnalysisReqeust;
import com.danawa.dsearch.server.document.dto.SearchQueryCreateRequest;
import com.danawa.dsearch.server.document.dto.SearchQueryUpdateRequest;
import com.danawa.dsearch.server.document.entity.SearchQuery;
import com.danawa.dsearch.server.excpetions.ElasticQueryException;
import com.danawa.dsearch.server.document.service.DocumentAnalysisService;
import com.danawa.dsearch.server.document.entity.RankingTuningRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;


@RestController
@RequestMapping("/document/analysis")
public class DocumentAnalysisController {
    private static Logger logger = LoggerFactory.getLogger(DocumentAnalysisController.class);
    private final DocumentAnalysisService documentAnalysisService;

    public DocumentAnalysisController(DocumentAnalysisService documentAnalysisService) {
        this.documentAnalysisService = documentAnalysisService;
    }

    @PostMapping
    public ResponseEntity<?> analyzeDocument(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody DocumentAnalysisReqeust documentAnalysisReqeust) {
        return new ResponseEntity<>(documentAnalysisService.analyzeDocument(clusterId, documentAnalysisReqeust), HttpStatus.OK);
    }
}
