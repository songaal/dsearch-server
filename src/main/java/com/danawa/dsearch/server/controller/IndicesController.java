package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.DocumentAnalyzer;
import com.danawa.dsearch.server.entity.DocumentPagination;
import com.danawa.dsearch.server.services.IndicesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/indices")
public class IndicesController {
    private static Logger logger = LoggerFactory.getLogger(IndicesController.class);

    private IndicesService indicesService;

    public IndicesController(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @GetMapping("/{index}/_docs")
    public ResponseEntity<?> findAllDocument(@PathVariable String index,
                                             @RequestHeader(value = "cluster-id") UUID clusterId,
                                             @RequestParam int pageNum,
                                             @RequestParam int rowSize,
                                             @RequestParam(required = false, defaultValue = "false") boolean analysis,
                                             @RequestParam(required = false) String id) throws IOException {

        DocumentPagination documentPagination = indicesService.findAllDocumentPagination(clusterId, index, pageNum, rowSize, id, analysis, null);

        return new ResponseEntity<>(documentPagination, HttpStatus.OK);
    }

    @PostMapping("/{index}/analyzer")
    public ResponseEntity<?> analyzer(@PathVariable String index,
                                      @RequestHeader(value = "cluster-id") UUID clusterId,
                                      @RequestBody Map<String, List<DocumentAnalyzer.Analyzer>> documents) throws IOException {
        Map<String, List<DocumentAnalyzer.Analyzer>> analyzerMap = new HashMap<>();
        try {
            analyzerMap = indicesService.getDocumentAnalyzer(clusterId, index, documents);
        } catch (Exception e) {
            logger.warn("", e.getMessage());
        }
        return new ResponseEntity<>(analyzerMap, HttpStatus.OK);
    }

}
