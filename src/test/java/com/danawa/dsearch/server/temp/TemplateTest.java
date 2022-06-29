package com.danawa.dsearch.server.temp;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.IndexTemplateMetadata;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TemplateTest {

    public static void main(String[] args) {
        try {
            HttpHost httpHost = new HttpHost("119.205.208.131", 9200);
            RestClientBuilder builder = RestClient.builder(httpHost)
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(10000)
                            .setSocketTimeout(60000))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultIOReactorConfig(
                            IOReactorConfig.custom()
                                    .setIoThreadCount(1)
                                    .build()
                    ));
            RestHighLevelClient client = new RestHighLevelClient(builder);
            GetIndexTemplatesRequest getIndexTemplatesRequest = new GetIndexTemplatesRequest("*-a", "*-b");
            GetIndexTemplatesResponse getIndexTemplatesResponse = client.indices().getIndexTemplate(getIndexTemplatesRequest, RequestOptions.DEFAULT);
            List<IndexTemplateMetadata> list = getIndexTemplatesResponse.getIndexTemplates();

            Set<String> names = new HashSet<>();
            for (IndexTemplateMetadata templateMetadata : list) {
                String name = templateMetadata.name().replace("-a", "").replace("-b", "");
                names.add(name);
            }

            int i = 1;
            for (String name : names) {
                System.out.println(i++ + " >> " + name);
            }

        } catch (Exception e) {
            System.err.println(e);
        }
    }

}
