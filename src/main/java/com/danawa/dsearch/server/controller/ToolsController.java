package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.DetailAnalysisRequest;
import com.danawa.dsearch.server.services.ToolsService;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URLDecoder;
import java.util.*;

@RestController
@RequestMapping("/tools")
public class ToolsController {
    private static Logger logger = LoggerFactory.getLogger(ToolsController.class);

    @Autowired
    ToolsService toolsService;

    public ToolsController() { }

    @GetMapping("/plugins")
    public ResponseEntity<?> getPlugins(@RequestHeader(value = "cluster-id") UUID clusterId) throws Exception {
        Map<String, Object> resultEntitiy = new HashMap<String, Object>();

        Response pluginResponse = toolsService.getPlugins(clusterId);
        String responseBody = EntityUtils.toString(pluginResponse.getEntity());
        String[] splitLists = responseBody.split("\n");
        Set<String> plugins = new HashSet<>();
        for(String list : splitLists){
            String[] items  = list.replaceAll(" +", " ").split(" ");

            if(items.length >= 2)
                plugins.add(items[1]);
        }

        List<String> result = toolsService.checkPlugins(clusterId, plugins);
        resultEntitiy.put("plugins", result);
        return new ResponseEntity<>(resultEntitiy, HttpStatus.OK);
    }

    @PostMapping("/detail/analysis")
    public ResponseEntity<?> getDetailAnalysis(@RequestHeader(value = "cluster-id") UUID clusterId,
                                               @RequestBody DetailAnalysisRequest detailAnalysisRequest) throws Exception {

        // text필드에 있는 특수문자 처리 :  ex.) 15"
        detailAnalysisRequest.setText(detailAnalysisRequest.getText().replace("\"", "\\\""));
        logger.info("{}", detailAnalysisRequest);
        Response response = toolsService.getDetailAnalysis(clusterId, detailAnalysisRequest);
        String responseBody = EntityUtils.toString(response.getEntity());

        // 관리도구 수정사항 - 2021-03-30
        Gson gson = new Gson();
        Map<String, Object> map = new HashMap<>();
        map = (Map<String, Object>) gson.fromJson(responseBody, map.getClass());

        // [0] : 전체 질의어
        // [1] : 불용어
        // [2] : 모델명 규칙
        // [3] : 단위명 규칙
        // [4] : 형태소 분리 결과
        // [5] : 동의어 확장
        // [6] : 복합 명사
        // [7] : 최종 결과
        List<Map<String, String>> result = (List<Map<String, String>>) map.get("result");

        // 형태소 분리 결과에 모델명 형태소 분석 포함
        Map<String, String> model = result.get(2);
        Map<String, String> morpheme = result.get(4);

        String modelStr = model.get("value");
        String morphemeStr = morpheme.get("value");
        if(modelStr.length() > 0){
            modelStr = modelStr.substring(modelStr.indexOf("(") + 1, modelStr.indexOf(")"));
            morphemeStr += ", " + modelStr;
            morpheme.replace("value", morphemeStr);
        }

        // 중복 형태소는 제거 (형태소 분리, 동의어 확장, 최종결과)
        Map<String, String> synonym = result.get(5);
        Map<String, String> finalResult = result.get(7);
        Set<String> removeSet = new LinkedHashSet<>();

        StringBuilder sb = new StringBuilder();
        // 1) 형태소 분리
        String str = morpheme.get("value");
        String[] strList = str.split(",");

        for( String item : strList){
            item = item.trim();
            removeSet.add(item);
        }

        for(String item : removeSet){
            if(sb.length() > 0) sb.append("," + item);
            else sb.append(item);
        }

        morpheme.replace("value", sb.toString());
        sb.setLength(0);

        // 2) 동의어 확장
        str = synonym.get("value");
        strList = str.split("<br/>");
        removeSet = new LinkedHashSet<>();
        for( String item : strList){
            item = item.trim();
            removeSet.add(item);
        }

        for(String item : removeSet){
            if(sb.length() > 0) sb.append("<br />" + item);
            else sb.append(item);
        }

        synonym.replace("value", sb.toString());
        sb.setLength(0);

        // 3) 최종 결과
        str = finalResult.get("value");
        strList = str.split(",");
        removeSet = new LinkedHashSet<>();
        for( String item : strList){
            item = item.trim();
            removeSet.add(item);
        }

        for(String item : removeSet){
            if(sb.length() > 0) sb.append("," + item);
            else sb.append(item);
        }

        finalResult.replace("value", sb.toString());
        sb.setLength(0);

        String mapString = gson.toJson(map);
        return new ResponseEntity<>(mapString, HttpStatus.OK);
    }
}

