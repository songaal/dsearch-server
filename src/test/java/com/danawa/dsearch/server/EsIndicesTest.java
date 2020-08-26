//package com.danawa.dsearch.server;
//
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.client.indices.GetFieldMappingsRequest;
//import org.elasticsearch.client.indices.GetFieldMappingsResponse;
//import org.elasticsearch.client.indices.GetMappingsRequest;
//import org.elasticsearch.client.indices.GetMappingsResponse;
//import org.elasticsearch.cluster.metadata.MappingMetaData;
//import org.junit.jupiter.api.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.boot.test.context.SpringBootTest;
//
//import java.io.IOException;
//import java.util.*;
//
//@SpringBootTest
//class EsIndicesTest {
//    private static Logger logger = LoggerFactory.getLogger(EsIndicesTest.class);
//
//    private RestHighLevelClient client;
//
//    public EsIndicesTest(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient) {
//        this.client = restHighLevelClient;
//    }
//
//
//    @Test
//    void getMapping() {
//        try {
////            GetFieldMappingsRequest request = new GetFieldMappingsRequest().indices("keyword");
////
////            GetFieldMappingsResponse response = client.indices().getFieldMapping(request, RequestOptions.DEFAULT);
////
////            logger.debug("{}", response);
//            String index = "keyword";
////            String index = "great1";
//            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
//            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
//            Map<String, MappingMetaData> mappings = response.mappings();
//            List<String> fields = new ArrayList<>();
//            Map<String, Object> sourceAsMap = mappings.get(index).getSourceAsMap();
//
//            if(sourceAsMap.get("properties") instanceof LinkedHashMap) {
//                Iterator<String> iterator = ((Map) sourceAsMap.get("properties")).keySet().iterator();
//                while(iterator.hasNext()) {
//                    fields.add(iterator.next());
//                }
//            }
//
//            logger.debug("{}", fields);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//
//
//}
