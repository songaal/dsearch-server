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
import org.springframework.web.multipart.MultipartFile;

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
        return new ResponseEntity<>(dictionaryService.getAnalysisPluginSettings(clusterId), HttpStatus.OK);
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
                                            @RequestBody Map<String, Object> request) throws IOException, ServiceException {
        request.put("type", dictionary);
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
                                            @RequestBody Map<String, Object> request) throws IOException {
        logger.info("{}", request);
        request.put("type", dictionary);
        return new ResponseEntity<>(dictionaryService.updateDocument(clusterId, id, request), HttpStatus.OK);
    }

    @GetMapping("/remote")
    public ResponseEntity<?> getRemote(@RequestHeader(value = "cluster-id") UUID clusterId) {
        return new ResponseEntity<>(dictionaryService.getRemoteInfo(clusterId), HttpStatus.OK);
    }

    @GetMapping("/summary")
    public ResponseEntity<?> getSummary(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        Map<String, Object> entity = new HashMap<String, Object>();
        List<DictionarySetting> dictionarySettings = dictionaryService.getAnalysisPluginSettings(clusterId);
        entity.put("dictionarySettings", dictionarySettings);
        return new ResponseEntity<>(entity, HttpStatus.OK);
    }

    @PostMapping("/find-dict")
    public ResponseEntity<?> searchDictionaries(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                @RequestBody Map<String, Object> request) throws IOException{

        logger.info("clusterId: {} request: {}", clusterId, request.toString());
        Map<String, Object> map = dictionaryService.getRemoteInfo(clusterId);

        boolean isRemote = (Boolean) map.get("remote");

        logger.info("Remote Info: {}", map);
        Response findDictResponse = null;
        if(isRemote){
            UUID remoteClusterId = (UUID) map.get("remoteClusterId");
            findDictResponse = dictionaryService.findDict(remoteClusterId, request);
        }else{
            findDictResponse = dictionaryService.findDict(clusterId, request);
        }

        String response = EntityUtils.toString(findDictResponse.getEntity());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/compile-dict")
    public ResponseEntity<?> compileDictionaries(@RequestHeader(value = "cluster-id") UUID clusterId,
                                                @RequestBody Map<String, Object> request) throws IOException{
        logger.info("clusterId: {}, compile-dict params: {}", clusterId, request.toString());
        Response compileDictResponse = dictionaryService.compileDict(clusterId, request);
        String response = EntityUtils.toString(compileDictResponse.getEntity());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping(value = "/fileUpload", headers = ("Content-Type=multipart/*"))
    public ResponseEntity<?> uploadFile(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @RequestParam("overwrite") boolean overwrite,
                                        @RequestParam("dictionaryName") String dictionaryName,
                                        @RequestParam("dictionaryType") String dictionaryType,
                                        @RequestParam("dictionaryFields") List<String> dictionaryList,
                                        @RequestParam("filename") MultipartFile file
    ) {
        Map<String, Object> result = null;

        logger.info("overwrite: {}, dictionaryName: {}, dictionaryType: {}, dictionaryFields: {}", overwrite, dictionaryName, dictionaryType, dictionaryList.toString());
        if(overwrite){ // 덮어쓰기
            dictionaryService.resetDict(clusterId, dictionaryName);
        }

//        dictionaryList => ["updatedTime", "createdTime", "id", "keyword", "type", "value"]
        dictionaryList.remove("createdTime");
        dictionaryList.remove("updatedTime");

        switch (dictionaryType.toLowerCase()){
            case "set":
//              "type" : "keyword",
//              "label" : "단어"
                dictionaryList.remove("id");
                dictionaryList.remove("value");
                dictionaryList.remove("type");
                result = dictionaryService.insertDictFileToIndex(clusterId, dictionaryName, dictionaryType.toLowerCase(), file, dictionaryList);
                break;
            case "space":
                dictionaryList.remove("id");
                dictionaryList.remove("value");
                dictionaryList.remove("type");
                result = dictionaryService.insertDictFileToIndex(clusterId, dictionaryName, dictionaryType.toLowerCase(), file, dictionaryList);
                break;
            case "synonym_2way":
                // "type" : "value",
                // "label" : "유사어"
                dictionaryList.remove("id");
                dictionaryList.remove("keyword");
                dictionaryList.remove("type");
                result = dictionaryService.insertDictFileToIndex(clusterId, dictionaryName, dictionaryType.toLowerCase(), file, dictionaryList);
                break;
            case "compound":
//                "type" : "keyword",
//                    "label" : "단어"
//                "type" : "value",
//                    "label" : "값"
                dictionaryList.remove("id");
                dictionaryList.remove("type");
                result = dictionaryService.insertDictFileToIndex(clusterId, dictionaryName, dictionaryType.toLowerCase(), file, dictionaryList);
                break;
            case "synonym":
//                "type" : "keyword",
//                    "label" : "키워드"
//                "type" : "value",
//                    "label" : "유사어"
                dictionaryList.remove("id");
                dictionaryList.remove("type");
                result = dictionaryService.insertDictFileToIndex(clusterId, dictionaryName, dictionaryType.toLowerCase(), file, dictionaryList);
                break;
            case "custom":
                dictionaryList.remove("type");
                result = dictionaryService.insertDictFileToIndex(clusterId, dictionaryName, dictionaryType.toLowerCase(), file, dictionaryList);
                break;
            default:
                result = new HashMap<>();
                result.put("result", false);
                result.put("message", "맞는 사전 타입이 없습니다.");
        }

        logger.info("{}", result);

        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @PostMapping(value = "/resetDict")
    public ResponseEntity<?> removeAllDictData(@RequestHeader(value = "cluster-id") UUID clusterId,
                                        @RequestParam("dictionaryName") String dictionaryName
    ) {
        logger.info("clusterId: {} dictionaryName: {}", clusterId, dictionaryName);
        dictionaryService.resetDict(clusterId, dictionaryName);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
