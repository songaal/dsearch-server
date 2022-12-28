package com.danawa.dsearch.server.documentAnalysis.controller;

import com.danawa.dsearch.server.documentAnalysis.dto.DocumentAnalysisDetailRequest;
import com.danawa.dsearch.server.documentAnalysis.dto.DocumentAnalysisReqeust;
import com.danawa.dsearch.server.documentAnalysis.service.DocumentAnalysisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
        Map<String, Object> data = documentAnalysisService.analyzeDocument(clusterId, documentAnalysisReqeust);
        return new ResponseEntity<>(data, HttpStatus.OK);
    }

    @PostMapping("/detail")
    public ResponseEntity<?> analyzeDocumentDetail(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                    @RequestBody DocumentAnalysisDetailRequest documentAnalysisDetailRequest) {
        Map<String, Object> data = documentAnalysisService.analyzeDocumentDetails(clusterId, documentAnalysisDetailRequest);
        logger.info("{}", data);
        return new ResponseEntity<>(data, HttpStatus.OK);
    }

}
