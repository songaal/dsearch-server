package com.danawa.dsearch.server.dynamicIndex.service;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Component
public class QueueIndexerClient {
    private RestTemplate restTemplate;

    public QueueIndexerClient(){
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        this.restTemplate = new RestTemplate(factory);
    }

    public ResponseEntity<?> get(URI url){
        return restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
    }

    public ResponseEntity<?> put(URI url, Map<String, Object> body){
        return restTemplate.exchange(url, HttpMethod.PUT, new HttpEntity<>(body), Map.class);
    }
}
