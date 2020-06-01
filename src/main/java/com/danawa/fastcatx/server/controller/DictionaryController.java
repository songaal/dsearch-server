package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.services.DictionaryService;
import com.danawa.fastcatx.server.services.IndicesService;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dictionaries")
public class DictionaryController {
    private static Logger logger = LoggerFactory.getLogger(DictionaryController.class);

    private DictionaryService dictionaryService;

    public DictionaryController(DictionaryService dictionaryService) {
        this.dictionaryService = dictionaryService;
    }

    @GetMapping("/{dictionary}/download")
    public ResponseEntity<?> download(@PathVariable String dictionary) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        List<SearchHit> documentList = dictionaryService.findAll(dictionary.toUpperCase());
        StringBuffer sb = new StringBuffer();
        int size = documentList.size();
        for (int i=0; i < size; i++) {
            Map<String, Object> source = documentList.get(i).getSourceAsMap();
            if ("USER".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
            }
            sb.append("\r\n");
        }
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + "text.txt");
        return new ResponseEntity<>(sb.toString(), headers, HttpStatus.OK);
    }

    @GetMapping("/{dictionary}")
    public ResponseEntity<?> dictionaryPagination(@PathVariable String dictionary,
                                                  @RequestParam(defaultValue = "0") long pageNum,
                                                  @RequestParam(defaultValue = "15") long rowSize,
                                                  @RequestParam(required = false) String searchField,
                                                  @RequestParam(required = false) String searchValue) throws IOException {

        DocumentPagination documentPagination = dictionaryService.documentPagination(dictionary.toUpperCase(), pageNum, rowSize, searchField, searchValue);
        return new ResponseEntity<>(documentPagination, HttpStatus.OK);
    }


}
