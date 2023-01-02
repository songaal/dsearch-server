package com.danawa.dsearch.server.documentAnalysis.service;

import com.danawa.dsearch.server.documentAnalysis.adapter.DocumentAnalysisElasticsearchAdapter;
import com.danawa.dsearch.server.documentAnalysis.dto.DocumentAnalysisDetailRequest;
import com.danawa.dsearch.server.documentAnalysis.dto.DocumentAnalysisReqeust;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.excpetions.NoSuchAnalyzerException;
import com.danawa.dsearch.server.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;


@Service
public class DocumentAnalysisService {
    /**
     * 이 서비스 레이어에서 Repository를 사용하지 않는 이유 ?
     * 1. 직접적으로 저장을 하는 부분이 없음
     * 2. ElasticsearchFactoryHighLevelWrapper 클래스 자체가 ES에 대해 래핑한 Util 클래스 이기 때문에 핵심 비지니스를 지킬 수 있어서
     */
    private static Logger logger = LoggerFactory.getLogger(DocumentAnalysisService.class);

    private DocumentAnalysisElasticsearchAdapter documentAnalysisElasticsearchAdapter;

    public DocumentAnalysisService(
            DocumentAnalysisElasticsearchAdapter documentAnalysisElasticsearchAdapter) {
        this.documentAnalysisElasticsearchAdapter = documentAnalysisElasticsearchAdapter;
    }

    public Map<String, Object> analyzeDocument(UUID clusterId, DocumentAnalysisReqeust documentAnalysisReqeust) {
        String index = documentAnalysisReqeust.getIndex();
        String query = documentAnalysisReqeust.getQuery();

        Map<String, Object> result = new HashMap<>();
        Map<String, Object> data = new HashMap<>();
        result.put("index", index);
        result.put("isSuccess", true);

        try {
            // 1. Index Mapping 가져오기
            Map<String, Object> mappings = documentAnalysisElasticsearchAdapter.getIndexMappings(clusterId, index);

            // 2. 분석기가 셋팅된 필드만 파싱
            Map<String, Object> extractedMappings = extractAnalyzerFields(index, mappings);

            // 3. 만약 분석기가 셋팅되어 있지 않다면 종료
            if (extractedMappings.size() == 0) {
                throw new NoSuchAnalyzerException("Not Found Analyzer In Index: "+ index);
            }

            // 4. 분석기가 셋팅되어 있다면 해당 필드만 검색
            String[] extractedFields = extractedMappings.keySet().toArray(new String[0]);

            // 5. 쿼리와 field 리스트를 합치기
            Map<String, Object> queryMap = JsonUtils.convertStringToMap(query);
            queryMap.put("size", 100); // 5-1. 1만건 쿼리는 너무 많은 리소스를 소모하므로 100건만
            queryMap.put("_source", extractedFields);

            // 6. 문자열 형태로 변환 후 검색
            String mergedQuery = JsonUtils.convertMapToString(queryMap);
            logger.info("{}", mergedQuery);
            List<Map<String, Object>> searchList = documentAnalysisElasticsearchAdapter.findAll(clusterId, index, mergedQuery);

            // 7. termvector로 해당 필드의 내용 분석
            for(Map<String, Object> item: searchList){
                String docId = (String) item.get("_id");
                Map<String, Object> source = (Map<String, Object>) item.get("_source");

                Map<String, Object> termVectors = documentAnalysisElasticsearchAdapter.getTerms(clusterId, index, docId, extractedFields);
                if (data.get(docId) == null){
                    data.put(docId, new HashMap<>());
                }

                makePrettyFormat((Map<String, Object>) data.get(docId), source, termVectors);
            }
        } catch (IOException e) {
            logger.error("", e);
            result.put("isSuccess", false);
        }catch (NoSuchAnalyzerException e){
            logger.error("", e);
            result.put("isSuccess", false);
        }

        // 8. 셋팅 후 리턴
        result.put("analysis", data);
        return result;
    }

    private Map<String, Object> extractAnalyzerFields(String index, Map<String, Object> mappings){
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> properties = (Map<String, Object>) mappings.get(index);
        Map<String, Object> fieldsMapping = (Map<String, Object>) properties.get("properties");

        for (String field: fieldsMapping.keySet()){
            Map<String, Object> metadata = (Map<String, Object>) fieldsMapping.get(field);
            if(isAnalyzerSet(metadata)){
                result.put(field, metadata);
            }
        }

        return result;
    }

    private boolean isAnalyzerSet(Map<String, Object> metadata){
        if( metadata.get("analyzer") != null){
            return true;
        }else if (metadata.get("search_analyzer") != null){
            return true;
        }
        return false;
    }


    public Map<String, Object> analyzeDocumentDetails(UUID clusterId, DocumentAnalysisDetailRequest documentAnalysisDetailRequest){
        String index = documentAnalysisDetailRequest.getIndex();
        String docId = documentAnalysisDetailRequest.getDocId();

        Map<String, Object> result = new HashMap<>();

        try {
            // 1. Index Mapping 가져오기
            Map<String, Object> mappings = documentAnalysisElasticsearchAdapter.getIndexMappings(clusterId, index);

            // 2. 분석기가 셋팅된 필드만 파싱
            Map<String, Object> extractedMappings = extractAnalyzerFields(index, mappings);
            String[] extractedFields = extractedMappings.keySet().toArray(new String[0]);

            // 3. 문서 한개만 검색
            Map<String, Object> source = documentAnalysisElasticsearchAdapter.findById(clusterId, index, docId);

            // 4. termvector로 해당 필드의 내용 분석
            Map<String, Object> termVectors = documentAnalysisElasticsearchAdapter.getTerms(clusterId, index, docId, extractedFields);

            makePrettyFormat(result, source, termVectors);
        } catch (IOException e) {
            logger.error("", e);
        }

        // 5. 셋팅 후 리턴
        return result;
    }

    private void makePrettyFormat(Map<String, Object> data, Map<String, Object> originSource, Map<String, Object> joinSource){
        // 포맷팅 작업
        // {
        //   "필드이름": {
        //     "document": "문자열",
        //     " ": [ "문자열1", "문자열2", ... ],
        //   }
        // }
        for(String fieldName : originSource.keySet()){
            Map<String, Object> fields = new HashMap<>();
            fields.put("document", originSource.get(fieldName));
            fields.put("documentTermVectors", joinSource.get(fieldName) == null ? "" : joinSource.get(fieldName));
            data.put(fieldName, fields);
        }
    }
}
