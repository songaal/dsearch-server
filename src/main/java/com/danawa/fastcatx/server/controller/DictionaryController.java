package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.CreateDictDocumentRequest;
import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.services.DictionaryService;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@CrossOrigin("*")
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
            } else if ("SYNONYM".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
                sb.append("\t");
                sb.append(source.get("synonym"));
            } else if ("STOP".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
            } else if ("SPACE".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
            } else if ("COMPOUND".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
                sb.append("\t");
                sb.append(source.get("synonym"));
            } else if ("UNIT".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
            } else if ("UNIT_SYNONYM".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("synonym"));
            } else if ("MAKER".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("id"));
                sb.append("\t");
                sb.append(source.get("keyword"));
                sb.append("\t");
                sb.append(source.get("synonym"));
            } else if ("BRAND".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("id"));
                sb.append("\t");
                sb.append(source.get("keyword"));
                sb.append("\t");
                sb.append(source.get("synonym"));
            } else if ("CATEGORY".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("id"));
                sb.append("\t");
                sb.append(source.get("keyword"));
                sb.append("\t");
                sb.append(source.get("synonym"));
            } else if ("ENGLISH".equalsIgnoreCase(dictionary)) {
                sb.append(source.get("keyword"));
            }
            sb.append("\r\n");
        }
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + dictionary +  ".txt");
        return new ResponseEntity<>(sb.toString(), headers, HttpStatus.OK);
    }

    @GetMapping("/{dictionary}")
    public ResponseEntity<?> dictionaryPagination(@PathVariable String dictionary,
                                                  @RequestParam(defaultValue = "0") long pageNum,
                                                  @RequestParam(defaultValue = "40") long rowSize,
                                                  @RequestParam(defaultValue = "false") boolean isMatch,
                                                  @RequestParam(required = false, defaultValue = "keyword.raw") String field,
                                                  @RequestParam(required = false) String value) throws IOException {

        DocumentPagination documentPagination = dictionaryService.documentPagination(dictionary.toUpperCase(), pageNum, rowSize, isMatch, field, value);
        return new ResponseEntity<>(documentPagination, HttpStatus.OK);
    }

    @PostMapping("/{dictionary}")
    public ResponseEntity<?> createDocument(@PathVariable String dictionary,
                                            @RequestBody CreateDictDocumentRequest request) throws IOException {
        request.setType(dictionary.toUpperCase());
        dictionaryService.createDocument(request);
        return new ResponseEntity<>(HttpStatus.OK);
    }


    @DeleteMapping("/{dictionary}/{id}")
    public ResponseEntity<?> deleteDocument(@PathVariable String dictionary,
                                            @PathVariable String id) throws IOException {

        return new ResponseEntity<>(dictionaryService.deleteDocument(id), HttpStatus.OK);
    }

}
