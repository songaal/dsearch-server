package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.entity.RankingTuningRequest;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class AnalyzeTest {

    public static void findAnalyzer(Map<String, String> analyzers, String key, Map<String, Object> linkedHashMap){
        for(String key2 : linkedHashMap.keySet()){
            if(key2.equals("analyzer")){
                analyzers.put(key, (String) linkedHashMap.get(key2));
            }else{
                if(linkedHashMap.get(key2) != null && linkedHashMap.get(key2) instanceof LinkedHashMap){
                    Map<String, Object> lMap = (LinkedHashMap) linkedHashMap.get(key2);
                    findAnalyzer(analyzers, key, lMap);
                }

            }
        }
    }
    public static void main(String[] args) {
        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 80, "http"));

        RankingTuningRequest rankingTuningRequest = new RankingTuningRequest();
        rankingTuningRequest.setIndex("prod_test4");
        rankingTuningRequest.setText("{\n" +
                " \"explain\": true,\n"+
                " \"from\": 0,\n"+
                " \"size\": 10,\n"+
                " \"query\": {\n" +
                "        \"match\" : {\n" +
                "            \"PRODUCTNAME\" : \"노트북\"\n" +
                "        }\n" +
                "    }\n" +
                "}");

        try {
            RestHighLevelClient client = new RestHighLevelClient(builder);

            String index = rankingTuningRequest.getIndex();
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetaData> mappings = response.mappings();
            Map<String, Object> properties = (Map<String, Object>) mappings.get(index).getSourceAsMap().get("properties");
            Map<String, Object> result = mappings.get(index).getSourceAsMap().get("properties") == null ? new HashMap<>() : properties;

            Map<String, String> analyzers = new LinkedHashMap<String, String>();
            for(String key : result.keySet()){
                Map<String, Object> linkedHashMap = (LinkedHashMap) result.get(key);
                findAnalyzer(analyzers, key, linkedHashMap);
            }

            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(rankingTuningRequest.getIndex());

            String query = rankingTuningRequest.getText();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()) , null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            for(int i = 0 ;i < searchHits.length; i++){
                Map<String, Object> sources = searchHits[i].getSourceAsMap();

                for(String key: sources.keySet()){
                    System.out.println(sources.get(key));
                }
            }

            client.close();
        } catch (IOException e) {
            System.out.println(e);
        }

    }


}
