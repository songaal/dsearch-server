package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.*;
import com.danawa.dsearch.server.excpetions.ServiceException;
import com.danawa.dsearch.server.services.DictionaryService;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/dictionaries")
public class DictionaryController {
    private static Logger logger = LoggerFactory.getLogger(DictionaryController.class);

    private DictionaryService dictionaryService;
    public DictionaryController(DictionaryService dictionaryService) {
        this.dictionaryService = dictionaryService;
    }

    @GetMapping("/settings")
    public ResponseEntity<?> settings(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        return new ResponseEntity<>(dictionaryService.getSettings(clusterId), HttpStatus.OK);
    }

    @PostMapping("/settings")
    public ResponseEntity<?> addSetting(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @RequestBody DictionarySetting setting) throws IOException {
        dictionaryService.addSetting(clusterId, setting);
        return new ResponseEntity<>(HttpStatus.OK);
    }
    @DeleteMapping("/settings/{id}")
    public ResponseEntity<?> removeSetting(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @PathVariable String id) throws IOException {
        dictionaryService.removeSetting(clusterId, id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/{dictionary}/download")
    public ResponseEntity<?> download(@RequestHeader(value = "cluster-id") UUID clusterId,
                                      @PathVariable String dictionary) throws IOException {
        HttpHeaders headers = new HttpHeaders();
        StringBuffer sb = dictionaryService.download(clusterId, dictionary);
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + dictionary +  ".txt");
        return new ResponseEntity<>(sb.toString(), headers, HttpStatus.OK);
    }

    @GetMapping("/{dictionary}")
    public ResponseEntity<?> dictionaryPagination(@PathVariable String dictionary,
                                                  @RequestHeader(value = "cluster-id") UUID clusterId,
                                                  @RequestParam(defaultValue = "0") long pageNum,
                                                  @RequestParam(defaultValue = "40") long rowSize,
                                                  @RequestParam(defaultValue = "false") boolean isMatch,
                                                  @RequestParam(required = false) String searchColumns,
                                                  @RequestParam(required = false) String value) throws IOException {

        DocumentPagination documentPagination = dictionaryService.documentPagination(clusterId, dictionary, pageNum, rowSize, isMatch, searchColumns, value);
        return new ResponseEntity<>(documentPagination, HttpStatus.OK);
    }

    @PostMapping("/{dictionary}")
    public ResponseEntity<?> createDocument(@PathVariable String dictionary,
                                            @RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestBody DictionaryDocumentRequest request) throws IOException, ServiceException {
        request.setType(dictionary);
        return new ResponseEntity<>(dictionaryService.createDocument(clusterId, request), HttpStatus.OK);
    }


    @DeleteMapping("/{dictionary}/{id}")
    public ResponseEntity<?> deleteDocument(@PathVariable String dictionary,
                                            @RequestHeader(value = "cluster-id") UUID clusterId,
                                            @PathVariable String id) throws IOException, ServiceException {
        return new ResponseEntity<>(dictionaryService.deleteDocument(clusterId, dictionary, id), HttpStatus.OK);
    }

    @PutMapping("/{dictionary}/{id}")
    public ResponseEntity<?> updateDocument(@PathVariable String dictionary,
                                            @PathVariable String id,
                                            @RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestBody DictionaryDocumentRequest request) throws IOException {
        request.setType(dictionary);
        return new ResponseEntity<>(dictionaryService.updateDocument(clusterId, id, request), HttpStatus.OK);
    }


    @GetMapping("/summary")
    public ResponseEntity<?> getSummary(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        Map<String, Object> entity = new HashMap<String, Object>();
        // 1. setting 필요
        List<DictionarySetting> dictionarySettings = dictionaryService.getSettings(clusterId);

        // 2. Dictionary 정보 필요 (_analysis-product-name/info-dict)
        String dictionaryInfo  = dictionaryService.getDictionaryInfo(clusterId);
        // 3. Time 가져오기
//        SearchResponse dictionaryTimes = dictionaryService.getDictionaryTimes(clusterId);

        entity.put("dictionarySettings", dictionarySettings);
        entity.put("dictionaryInfo", dictionaryInfo);
//        entity.put("dictionaryTimes", dictionaryTimes);

        // 전송
        return new ResponseEntity<>(entity, HttpStatus.OK);
    }

    @PostMapping("/find-dict")
    public ResponseEntity<?> searchDictionaries(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                @RequestBody DictionarySearchRequest dictionarySearchRequest) throws IOException{

        Response findDictResponse = dictionaryService.findDict(clusterId, dictionarySearchRequest);
        String response = EntityUtils.toString(findDictResponse.getEntity());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/compile-dict")
    public ResponseEntity<?> compileDictionaries(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                @RequestBody DictionaryCompileRequest dictionaryCompileRequest) throws IOException{

        System.out.println(dictionaryCompileRequest.getType());
        Response compileDictResponse = dictionaryService.compileDict(clusterId, dictionaryCompileRequest);
        String[] ids = dictionaryCompileRequest.getIds().trim().split(",");
        for(String id : ids){
            dictionaryService.updateTime(clusterId, id);
        }
        String response = EntityUtils.toString(compileDictResponse.getEntity());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/settings/updateList")
    public ResponseEntity<?> updatedSettingsList(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                 @RequestBody List<DictionarySetting> dictionarySettings
                                                 ) throws IOException {
        if(dictionarySettings.size() == 0){
            return new ResponseEntity<>(HttpStatus.METHOD_NOT_ALLOWED);
        }
        dictionaryService.updatedSettingsList(clusterId, dictionarySettings);
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
