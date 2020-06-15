package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.DictionaryDocumentRequest;
import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.excpetions.ServiceException;
import com.danawa.fastcatx.server.services.DictionaryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@CrossOrigin("*")
@RestController
@RequestMapping("/dictionaries")
public class DictionaryController {
    private static Logger logger = LoggerFactory.getLogger(DictionaryController.class);

    private DictionaryService dictionaryService;

    public DictionaryController(DictionaryService dictionaryService) {
        this.dictionaryService = dictionaryService;
    }

    @GetMapping("/settings")
    public ResponseEntity<?> settings() throws IOException {
        return new ResponseEntity<>(dictionaryService.getSettings(), HttpStatus.OK);
    }

    @GetMapping("/{dictionary}/download")
    public ResponseEntity<?> download(@PathVariable String dictionary) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        StringBuffer sb = dictionaryService.download(dictionary);
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + dictionary +  ".txt");
        return new ResponseEntity<>(sb.toString(), headers, HttpStatus.OK);
    }

    @GetMapping("/{dictionary}")
    public ResponseEntity<?> dictionaryPagination(@PathVariable String dictionary,
                                                  @RequestParam(defaultValue = "0") long pageNum,
                                                  @RequestParam(defaultValue = "40") long rowSize,
                                                  @RequestParam(defaultValue = "false") boolean isMatch,
                                                  @RequestParam(required = false) String searchColumns,
                                                  @RequestParam(required = false) String value) throws IOException {

        DocumentPagination documentPagination = dictionaryService.documentPagination(dictionary.toUpperCase(), pageNum, rowSize, isMatch, searchColumns, value);
        return new ResponseEntity<>(documentPagination, HttpStatus.OK);
    }

    @PostMapping("/{dictionary}")
    public ResponseEntity<?> createDocument(@PathVariable String dictionary,
                                            @RequestBody DictionaryDocumentRequest request) throws IOException, ServiceException {
        request.setType(dictionary.toUpperCase());
        return new ResponseEntity<>(dictionaryService.createDocument(request), HttpStatus.OK);
    }


    @DeleteMapping("/{dictionary}/{id}")
    public ResponseEntity<?> deleteDocument(@PathVariable String dictionary,
                                            @PathVariable String id) throws IOException, ServiceException {
        return new ResponseEntity<>(dictionaryService.deleteDocument(dictionary.toUpperCase(), id), HttpStatus.OK);
    }

    @PutMapping("/{dictionary}/{id}")
    public ResponseEntity<?> updateDocument(@PathVariable String dictionary,
                                            @PathVariable String id,
                                            @RequestBody DictionaryDocumentRequest request) throws IOException {
        request.setType(dictionary.toUpperCase());
        return new ResponseEntity<>(dictionaryService.updateDocument(id, request), HttpStatus.OK);
    }
}
