package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.services.DictionaryService;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
        this.dictionaryService  = dictionaryService;
    }


    @GetMapping("/{type}/download")
    public ResponseEntity<?> download(@PathVariable String type) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        List<SearchHit> documentList = dictionaryService.findAll(type.toUpperCase());
        StringBuffer sb = new StringBuffer();
        int size = documentList.size();
        for (int i=0; i < size; i++) {
            Map<String, Object> source = documentList.get(i).getSourceAsMap();
            if ("USER".equalsIgnoreCase(type)) {
                sb.append(source.get("keyword"));
            }
            sb.append("\r\n");
        }
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + "text.txt");
        return new ResponseEntity<>(sb.toString(), headers, HttpStatus.OK);

    }


}
