package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.Cluster;
import com.danawa.fastcatx.server.entity.ClusterStatusResponse;
import com.danawa.fastcatx.server.entity.ClusterStatusRequest;
import com.danawa.fastcatx.server.excpetions.NotFoundException;
import com.danawa.fastcatx.server.excpetions.NotFoundUserException;
import com.danawa.fastcatx.server.repository.ClusterRepository;
import com.danawa.fastcatx.server.repository.ClusterRepositorySupport;
import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class ClusterService {
    private static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private final ClusterRepository clusterRepository;
    private final ClusterRepositorySupport clusterRepositorySupport;

    public ClusterService(ClusterRepository clusterRepository,
                          ClusterRepositorySupport clusterRepositorySupport) {
        this.clusterRepository = clusterRepository;
        this.clusterRepositorySupport = clusterRepositorySupport;
    }

    public List<Cluster> findAll() {
        return clusterRepository.findAll();
    }

    public Cluster find(UUID id) {
        return clusterRepository.findById(id).get();
    }

    public Cluster add(Cluster cluster) {
        cluster.setId(null);
        Cluster registerCluster = clusterRepository.save(cluster);
        clusterRepository.flush();
        return registerCluster;
    }

    public Cluster remove(UUID id) {
        Cluster registerCluster = clusterRepository.findById(id).get();
        clusterRepository.delete(registerCluster);
        return registerCluster;
    }

    public Cluster edit(UUID id, Cluster cluster) throws NotFoundException {
        Cluster registerCluster = clusterRepository.findById(id).get();
        if (registerCluster == null) {
            throw new NotFoundException("Not Found Cluster");
        }
        registerCluster.setName(cluster.getName());
        registerCluster.setScheme(cluster.getScheme());
        registerCluster.setHost(cluster.getHost());
        registerCluster.setPort(cluster.getPort());
        registerCluster.setUsername(cluster.getUsername());
        registerCluster.setPassword(cluster.getPassword());
        registerCluster.setTheme(cluster.getTheme());
        registerCluster.setUpdateDate(LocalDateTime.now());
        registerCluster.setKibana(cluster.getKibana());
        return clusterRepository.save(registerCluster);
    }

    public ClusterStatusResponse scanClusterStatus(String scheme, String host, int port, String username, String password) {
        HttpHost[] HttpHost = new HttpHost[1];
        HttpHost[0] = new HttpHost(host, port, scheme);
        return scanClusterStatus(HttpHost, username, password);
    }
    public ClusterStatusResponse scanClusterStatus(ClusterStatusRequest clusterStatusRequest) {
        HttpHost[] HttpHost = new HttpHost[1];
        HttpHost[0] = new HttpHost(clusterStatusRequest.getHost(), clusterStatusRequest.getPort(), clusterStatusRequest.getScheme());
        return scanClusterStatus(HttpHost, clusterStatusRequest.getUsername(), clusterStatusRequest.getPassword());
    }
    public ClusterStatusResponse scanClusterStatus(HttpHost[] httpHost, String username, String password) {
        ClusterStatusResponse status;
        RestHighLevelClient client = null;
        RestClient restClient = null;
        try {
            RestClientBuilder builder = RestClient.builder(httpHost);
            if (username != null && !"".equals(username)
                    && password != null && !"".equals(password)) {
                final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
            }
            client = new RestHighLevelClient(builder);
            restClient = client.getLowLevelClient();

            Request nodesRequest = new Request("GET", "/_nodes");
            nodesRequest.addParameter("format", "json");
            nodesRequest.addParameter("human", "true");
            nodesRequest.addParameter("filter_path", "**.http.publish_address");
            String nodesResponse = convertResponseToString(restClient.performRequest(nodesRequest));
            Map<String, Object> nodes = (Map<String, Object>) new Gson().fromJson(nodesResponse, Map.class).get("nodes");

            Request stateRequest = new Request("GET", "/_stats");
            stateRequest.addParameter("format", "json");
            stateRequest.addParameter("human", "true");
            stateRequest.addParameter("filter_path", "_shards,_all.total.store,indices.*.uuid");
            String stateResponse = convertResponseToString(restClient.performRequest(stateRequest));
            Map<String, Object> state = new Gson().fromJson(stateResponse, Map.class);

            state.put("shards", ((Map)state.get("_shards")).get("total"));
            state.put("store", ((Map)((Map)((Map)state.get("_all")).get("total")).get("store")).get("size"));
            state.put("indices", ((Map) state.get("indices")).size());
            state.remove("_all");
            state.remove("_shards");
            status = new ClusterStatusResponse(true, nodes, state);
        } catch (Exception e) {
            logger.error("", e);
            status = new ClusterStatusResponse();
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
        }
        return status;
    }

    private String convertResponseToString(Response response) {
        String responseBody = null;
        try {
            responseBody = EntityUtils.toString(response.getEntity());
        } catch (IOException e) {
            logger.error("", e);
        }
        return responseBody;
    }

}
