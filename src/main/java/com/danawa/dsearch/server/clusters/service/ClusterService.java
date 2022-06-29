package com.danawa.dsearch.server.clusters.service;

import com.danawa.dsearch.server.clusters.entity.Cluster;
import com.danawa.dsearch.server.clusters.entity.ClusterStatusRequest;
import com.danawa.dsearch.server.clusters.entity.ClusterStatusResponse;
import com.danawa.dsearch.server.clusters.repository.ClusterRepository;
import com.danawa.dsearch.server.clusters.repository.ClusterRepositorySupport;
import com.danawa.dsearch.server.excpetions.NotFoundException;
import com.google.gson.*;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;

import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class ClusterService {
    private static Logger logger = LoggerFactory.getLogger(ClusterService.class);
    private final ClusterRepository clusterRepository;

    public ClusterService(ClusterRepository clusterRepository) {
        this.clusterRepository = clusterRepository;
    }

    public List<Cluster> findAll() {
        List<Cluster> clusterList = clusterRepository.findAll();
        return clusterList == null ? new ArrayList<>() : clusterList;
    }

    public List<Cluster> findByHostAndPort(String host, int port) {
        List<Cluster> clusterList = clusterRepository.findByHostAndPort(host, port);
        return clusterList == null ? new ArrayList<>() : clusterList;
    }

    public Cluster find(UUID id) {
        try{
            return clusterRepository.findById(id).get();
        }catch (NoSuchElementException e){
            return null;
        }
    }

    public Cluster add(Cluster cluster) throws NullPointerException {
        if(cluster == null) throw new NullPointerException("Cluster가 null 입니다");
        cluster.setId(null);
        Cluster registerCluster = clusterRepository.save(cluster);
        clusterRepository.flush();
        return registerCluster;
    }

    public Cluster remove(UUID id) throws NoSuchElementException{
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
        registerCluster.setDictionaryRemoteClusterId(cluster.getDictionaryRemoteClusterId());
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

            state.put("shards", ((Map) state.get("_shards")).get("total"));
            if (state.get("_all") != null) {
                state.put("store", ((Map) ((Map) ((Map) state.get("_all")).get("total")).get("store")).get("size"));
            } else {
                state.put("store", "0");
            }
            if (state.get("indices") != null) {
                state.put("indices", ((Map) state.get("indices")).size());
            } else {
                state.put("indices", "0");
            }

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

    public Cluster saveUrl(UUID id, Cluster cluster, String url) throws NotFoundException {
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
        registerCluster.setAutocompleteUrl(url);
        return clusterRepository.save(registerCluster);
    }

    public Map<String, Object> uploadFile(UUID clusterId, MultipartFile file) {
        Cluster cluster = find(clusterId);

        String scheme = cluster.getScheme();
        String host = cluster.getHost();
        int port = cluster.getPort();

        String username = cluster.getUsername();
        String password = cluster.getPassword();

        HttpHost[] httpHost = new HttpHost[1];
        httpHost[0] = new HttpHost(host, port, scheme);

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

        RestHighLevelClient client = new RestHighLevelClient(builder);
        client.getLowLevelClient();

        Map<String, Object> result = new HashMap<>();

        String line = null;


        try {
            InputStream in = file.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(in);
            BufferedReader br = new BufferedReader(new InputStreamReader(bis));
            CustomGsonAdapter adapter = new CustomGsonAdapter();
            Gson gson = new GsonBuilder().registerTypeAdapter(Map.class, adapter).create();

            while ((line = br.readLine()) != null) {
                Map<String, Object> body = gson.fromJson(line, Map.class);
                String index = (String) body.get("_index");
                String id = (String) body.get("_id");
                IndexRequest request = new IndexRequest(index);
                request.id(id);
                request.type("_doc");
                String source = gson.toJson(body.get("_source"));
                logger.info("index: {}, id: {}, source: {}", index, id, source);
                request.source(source, XContentType.JSON);
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                logger.info("{}", response);
            }

            client.close();
            br.close();
            bis.close();
            in.close();
        } catch (IOException e) {
            logger.info("{}", e);
            result.put("result", false);
            result.put("message", "failed IOException");
            return result;
        }
        result.put("result", true);
        result.put("message", "success");

        return result;
    }

    private class CustomGsonAdapter extends TypeAdapter<Object> {
        private final TypeAdapter<Object> delegate = new Gson().getAdapter(Object.class);

        @Override
        public void write(JsonWriter out, Object value) throws IOException {
            delegate.write(out, value);
        }

        @Override
        public Object read(JsonReader in) throws IOException {
            JsonToken token = in.peek();
            switch (token) {
                case BEGIN_ARRAY:
                    List<Object> list = new ArrayList<Object>();
                    in.beginArray();
                    while (in.hasNext()) {
                        list.add(read(in));
                    }
                    in.endArray();
                    return list;

                case BEGIN_OBJECT:
                    Map<String, Object> map = new LinkedTreeMap<String, Object>();
                    in.beginObject();
                    while (in.hasNext()) {
                        map.put(in.nextName(), read(in));
                    }
                    in.endObject();
                    return map;

                case STRING:
                    return in.nextString();

                case NUMBER:
                    //return in.nextDouble();
                    String n = in.nextString();
                    if (n.indexOf('.') != -1) {
                        return Double.parseDouble(n);
                    }
                    return Long.parseLong(n);
                case BOOLEAN:
                    return in.nextBoolean();
                case NULL:
                    in.nextNull();
                    return null;
                default:
                    throw new IllegalStateException();
            }
        }
    }

}
