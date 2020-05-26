package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.services.IndicesService;
import org.elasticsearch.action.search.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@CrossOrigin("*")
@RequestMapping("/indices")
public class IndicesController {
    private static Logger logger = LoggerFactory.getLogger(IndicesController.class);

    private IndicesService indicesService;

    public IndicesController(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @GetMapping("/{indices}/_docs")
    public ResponseEntity<?> findAllDocument(@PathVariable String indices,
                                             @RequestParam(defaultValue = "0") int from,
                                             @RequestParam(defaultValue = "100") int size,
                                             @RequestParam(required = false) String id) throws IOException {
        DocumentPagination documentPagination = indicesService.findAllDocumentPagination(indices, from, size, id);
        return new ResponseEntity<>(documentPagination, HttpStatus.OK);
    }


}
