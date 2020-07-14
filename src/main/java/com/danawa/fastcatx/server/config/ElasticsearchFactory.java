package com.danawa.fastcatx.server.config;

import com.danawa.fastcatx.server.entity.Cluster;
import com.danawa.fastcatx.server.services.ClusterService;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class ElasticsearchFactory {
    private static Logger logger = LoggerFactory.getLogger(ElasticsearchFactory.class);
    private final ClusterService clusterService;

    public ElasticsearchFactory(ClusterService clusterService) {
        this.clusterService = clusterService;
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
        RestClientBuilder builder = RestClient.builder(httpHost).setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultIOReactorConfig(
                IOReactorConfig.custom()
                        .setIoThreadCount(1)
                        .build() ));

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
        HttpHost[] httpHosts = null;
        try {
            Request request = new Request("GET", "/_cat/nodes");
            request.addParameter("format", "json");
            request.addParameter("h", "http");
            Response response = getClient(username, password, httpHost)
                    .getLowLevelClient()
                    .performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            List HostList = new Gson().fromJson(responseBody, List.class);

            httpHosts = new HttpHost[HostList.size() + 1];
            httpHosts[0] = httpHost;
            for (int i = 0; i < HostList.size(); i++) {
                Map<String, Object> hostMap = (Map<String, Object>) HostList.get(i);
                String[] host = ((String) hostMap.get("http")).split(":");
                httpHosts[i + 1] = new HttpHost(host[0], Integer.parseInt(host[1]), httpHost.getSchemeName());
            }
        } catch (IOException e) {
            logger.error("", e);
        }
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
