package com.danawa.dsearch.server.dynamicIndex.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Component
public class QueueIndexerClient {
    private static Logger logger = LoggerFactory.getLogger(QueueIndexerClient.class);
    private RestTemplate restTemplate;

    public QueueIndexerClient(){
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(1000);
        this.restTemplate = new RestTemplate(factory);
    }

    public QueueIndexerClient(RestTemplate restTemplate){
        this.restTemplate = restTemplate;
    }

    public Map<String, Object> get(URI url){
        try{
            ResponseEntity<?> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
            return (Map<String, Object>) responseEntity.getBody();
        }catch (Exception e){
            logger.info("{}", e);
            return new HashMap<>();
        }
    }

    public Map<String, Object> put(URI url, Map<String, Object> body){
        try{
            ResponseEntity<?> responseEntity = restTemplate.exchange(url, HttpMethod.PUT, new HttpEntity<>(body), Map.class);
            return (Map<String, Object>) responseEntity.getBody();
        }catch (Exception e){
            logger.info("{}", e);
            return new HashMap<>();
        }
    }
}
