package com.danawa.dsearch.server.temp;

import com.danawa.dsearch.server.rankingtuning.entity.AnalyzerTokens;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class AnalyzeTest {

    public static void findAnalyzer(Map<String, Object> analyzers, String property, Map<String, Object> linkedHashMap){
        for(String key : linkedHashMap.keySet()){
            if(key.equals("analyzer")){
                analyzers.put(property, linkedHashMap.get(key));
            }else{
                if(linkedHashMap.get(key) != null && linkedHashMap.get(key) instanceof LinkedHashMap){
                    Map<String, Object> map = (LinkedHashMap) linkedHashMap.get(key);
                    findAnalyzer(analyzers, property, map);
                }
            }
        }
    }
    public static AnalyzeResponse getMultipleAnalyze(RestHighLevelClient client, String index, String analyzer, String hitValue) throws IOException{
            AnalyzeRequest analyzeRequest = AnalyzeRequest.withIndexAnalyzer(index, analyzer, hitValue);
            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
            return analyzeResponse;
    }

    public static void main(String[] args) throws IOException{
        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 80, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        try {

//            RestClient restClient = client.getLowLevelClient();
//            Request request = new Request("GET", "/_analysis-product-name/info-dict");
//            Response response = restClient.performRequest(request);
//            String responseBody = EntityUtils.toString(response.getEntity());
//            System.out.println(responseBody);


            /* mappings */
            String index = "prod_test4";
            Map<String, Object> analyzers = new LinkedHashMap<String, Object>();
            GetMappingsRequest getMappingsRequest = new GetMappingsRequest().indices(index);
            GetMappingsResponse response = client.indices().getMapping(getMappingsRequest, RequestOptions.DEFAULT);
            Map<String, MappingMetadata> mappings = response.mappings();

            /* analyzer 뽑기 */

            // key: index, value: property
            for(String _index : mappings.keySet()){
                /* 인덱스 매핑에 있는 analyzer 가져오기 */
                LinkedHashMap<String, Object> mapping = (LinkedHashMap) mappings.get(_index).getSourceAsMap().get("properties");
                Map<String, Object> indexAnalyzer = new LinkedHashMap<String, Object>();
                if(mapping != null){
                    for(String property: mapping.keySet()){
                        LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap) mapping.get(property);
                        findAnalyzer(indexAnalyzer, property, linkedHashMap);
                    }
                }
                analyzers.put(_index, indexAnalyzer);
            }

            /* search */
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices(index);
            String query = "{}";
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.emptyList());
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(new NamedXContentRegistry(searchModule.getNamedXContents()), null, query)) {
                searchSourceBuilder.parseXContent(parser);
                searchRequest.source(searchSourceBuilder);
            }
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<Map<String, Object>> searchHitsResponse = new ArrayList<>();
            /* analyze */
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            Map<String, Map<String, List<AnalyzerTokens>>> analyzerTokensMap = new HashMap<>();
            for(int i = 0 ;i < searchHits.length; i++) {
                String hitsIndex = searchHits[i].getIndex();
                Map<String, List<AnalyzerTokens>> docWithTokensMap = new HashMap<>();
                List<AnalyzerTokens> analyzerTokensList = new ArrayList<>();

                Map<String, Object> source = searchHits[i].getSourceAsMap();
                searchHitsResponse.add(source);
                if(analyzers.get(hitsIndex) != null){
                    Map<String, Object> indexAnalyzer = (Map<String, Object>) analyzers.get(hitsIndex);

                    for(String field: source.keySet()){
                        if(indexAnalyzer.get(field) != null){
                            System.out.println(field);
                            AnalyzeResponse analyzeResponse = getMultipleAnalyze(client, hitsIndex, (String) indexAnalyzer.get(field), (String) source.get(field));
                            AnalyzerTokens analyzerTokens = new AnalyzerTokens();
                            analyzerTokens.setField(field);
                            List<String> tokens = new ArrayList<>();
                            for (AnalyzeResponse.AnalyzeToken token : analyzeResponse.getTokens()) {
                                tokens.add(token.getTerm());
                            }
                            analyzerTokens.setTokens(tokens);
                            analyzerTokensList.add(analyzerTokens);
                        }
                    }
                }
                source.put("_index", hitsIndex );
                source.put("_id", searchHits[i].getId());
                source.put("_explanation", searchHits[i].getExplanation());
                docWithTokensMap.put(searchHits[i].getId(), analyzerTokensList);
                analyzerTokensMap.put(hitsIndex, docWithTokensMap);
            }

            

            /* 리스트 형태로 합치기 */
            List<Object> result = new ArrayList<>();
            for(int i = 0; i < searchHitsResponse.size(); i++){
                List<Object> convertList = new ArrayList<>();
                Map<String, Object> item =  searchHitsResponse.get(i);

                for(String key : item.keySet()){
                    String id = (String) item.get("_id");
                    String _index = (String) item.get("_index");
                    Object text = item.get(key);

                    if(analyzerTokensMap.get(_index) != null){
                        List<AnalyzerTokens> analyzerFieldList = (ArrayList<AnalyzerTokens> ) analyzerTokensMap.get(_index).get(id);
                        AnalyzerTokens analyzerTokens = null;

                        if(analyzerFieldList == null){
                            Map<String, Object> data = new HashMap<>();
                            data.put("field", key);
                            data.put("text", text);
                            data.put("tokens",new ArrayList<>() );
                            convertList.add(data);
                            continue;
                        }
                        for(int j = 0; j < analyzerFieldList.size(); j++) {
                            if(analyzerFieldList.get(j).getField().equals(key)) {
                                analyzerTokens = analyzerFieldList.get(j);
                                break;
                            }
                        }

                        if(analyzerTokens == null || analyzerTokens.getTokens() == null || analyzerTokens.getTokens().size() == 0) {
                            Map<String, Object> data = new HashMap<>();
                            data.put("field", key);
                            data.put("text", text);
                            data.put("tokens", new ArrayList<>() );
                            convertList.add(data);
                        } else {
                            Map<String, Object> data = new HashMap<>();
                            data.put("field", key);
                            data.put("text", text);
                            data.put("tokens",analyzerTokens.getTokens() );
                            convertList.add(data);
                        }
                    }
                }
                result.add(convertList);
            }

            System.out.println();
        } catch (Exception e) {
            System.out.println(e);
        }
        client.close();
    }
}
