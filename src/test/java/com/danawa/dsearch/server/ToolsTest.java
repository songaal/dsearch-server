package com.danawa.dsearch.server;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ToolsTest {
    public static void main(String[] args) throws IOException{
        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 80, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);



        String dictionaryIndex = ".dsearch_dict";
        RestClient restClient = client.getLowLevelClient();



        /* 플러그인 가져오기 Start */
        Request request = new Request("GET", "_cat/plugins");
        Response response = restClient.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());

        String[] splitLists = responseBody.split("\n");
        Set<String> plugins = new HashSet<>();
        for(String list : splitLists){
            String[] items  = list.replaceAll(" +", " ").split(" ");

            if(items.length >= 2)
                plugins.add(items[1]);
        }
        /* 플러그인 가져오기 End */



        /* 플러그인에 analyze가 있는지 확인 Start */
        List<String> result = new ArrayList<>();
        for(String plugin : plugins){
            String method = "POST";
            String endPoint = "/_" +plugin + "/analyze";
            String setJson = "{ \"index\": \"" + dictionaryIndex + "\", \n" +
                    "\"detail\": true, \n" +
                    "\"useForQuery\": false, \n" +
                    "\"text\": \"Sandisk Extream Z80 USB 16gb\"}";


            try{
                Request pluginRequest = new Request(method, endPoint);
                pluginRequest.setJsonEntity(setJson);
                Response pluginResponse = restClient.performRequest(pluginRequest);
                result.add(plugin);
            }catch(ResponseException re){
                System.out.println(re);
            }
        }
        /* 플러그인에 analyze가 있는지 확인 End */



        /* return 해줄 것 */
        for(String plugin: result){
            System.out.println(plugin);
        }

        client.close();
    }
}

