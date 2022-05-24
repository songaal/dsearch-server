package com.danawa.dsearch.server.config;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.ClusterService;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.UUID;

@Component
public class ElasticsearchFactory {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFactory.class);
    private final ClusterService clusterService;
    private final RestTemplate restTemplate;

    public ElasticsearchFactory(ClusterService clusterService) {
        this.clusterService = clusterService;

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(3 * 1000);
        factory.setReadTimeout(3 * 1000);
        this.restTemplate = new RestTemplate(factory);
    }

    public UUID getDictionaryRemoteClusterId(UUID id) {
        Cluster cluster = clusterService.find(id);
        if (cluster.getDictionaryRemoteClusterId() != null && !"".equals(cluster.getDictionaryRemoteClusterId().trim())) {
            return UUID.fromString(cluster.getDictionaryRemoteClusterId());
        } else {
            return id;
        }
    }

    public RestHighLevelClient getClient(UUID id) {
        Cluster cluster = clusterService.find(id);

        String scheme = cluster.getScheme();
        String host = cluster.getHost();
        int port = cluster.getPort();

        String username = cluster.getUsername();
        String password = cluster.getPassword();

        HttpHost[] httpHosts = getHttpHostList(username, password, new HttpHost(host, port, scheme));
        return getClient(username, password, httpHosts);
    }

    public RestHighLevelClient getClient(String username, String password, HttpHost ...httpHost) {
        RestClientBuilder builder = RestClient.builder(httpHost)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(10000)
                        .setSocketTimeout(10 * 60 * 1000))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom()
                                .setIoThreadCount(1)
                                .build()
                ));

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (username != null && !"".equals(username)
                && password != null && !"".equals(password)) {
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        return new RestHighLevelClient(builder);
    }

    private HttpHost[] getHttpHostList(String username, String password, HttpHost httpHost) {
        HttpHost[] httpHosts = new HttpHost[1];
        httpHosts[0] = httpHost;
        return httpHosts;
    }

    public void close(RestHighLevelClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                logger.error("", e);
            }
        }
    }
}
