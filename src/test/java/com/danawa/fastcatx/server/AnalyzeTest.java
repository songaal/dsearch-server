package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.entity.RankingTuningRequest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
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
    public static void main(String[] args) {
        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 80, "http"));
        try {
            RestHighLevelClient client = new RestHighLevelClient(builder);
            RestClient restClient = client.getLowLevelClient();
            Request request = new Request("GET", "/_analysis-product-name/info-dict");
            Response response = restClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            System.out.println(responseBody);
        } catch (IOException e) {
            System.out.println(e);
        }

    }


}
