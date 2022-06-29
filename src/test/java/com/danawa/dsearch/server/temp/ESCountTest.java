package com.danawa.dsearch.server.temp;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ESCountTest {

    public static void main(String[] args) {

        RestClientBuilder builder = RestClient.builder(new HttpHost("es2.danawa.io", 9200))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(10000)
                        .setSocketTimeout(10 * 60 * 1000))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom()
                                .setIoThreadCount(1)
                                .build()
                ));

        RestHighLevelClient client = new RestHighLevelClient(builder);

        String index = "s-prod-v0-a";
        long startTime = 1615885848393L;
        String status = "SUCCESS";
        String jobType = "FULL_INDEX";

        BoolQueryBuilder countQuery = QueryBuilders.boolQuery();
        countQuery.filter().add(QueryBuilders.termQuery("index", index));
        countQuery.filter().add(QueryBuilders.termQuery("startTime", startTime));
        countQuery.filter().add(QueryBuilders.termQuery("status", status));
        countQuery.filter().add(QueryBuilders.termQuery("jobType", jobType));
        try {
            CountResponse countResponse = client.count(new CountRequest(".dsearch_index_history").query(countQuery), RequestOptions.DEFAULT);

            System.out.println(countResponse.getCount());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
