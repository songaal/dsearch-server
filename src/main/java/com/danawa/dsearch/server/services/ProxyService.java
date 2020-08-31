package com.danawa.dsearch.server.services;

import org.springframework.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.*;


@Service
public class ProxyService {
    private static Logger logger = LoggerFactory.getLogger(ProxyService.class);

    public ProxyService( ) {

    }

    public ResponseEntity<byte[]> proxy(String url, String keyword) throws UnsupportedEncodingException {
        ResponseEntity response = null;
        url += URLEncoder.encode(keyword, "UTF-8");
        System.out.println(url);
        logger.debug("{}", url);
        HttpEntity httpEntity = new HttpEntity<>(url);
        RestTemplate restTemplate = new RestTemplate();
        try {
            response = restTemplate.exchange(new URI(url), HttpMethod.GET, httpEntity, byte[].class);
        } catch (RestClientException | URISyntaxException e) {
            logger.debug("proxy fail : {}", e.getMessage());
        }

        if(response == null){
            Map<String, Object> map = new HashMap<>();
            map.put("q", keyword);
            map.put("result", new ArrayList<>());
            map.put("action", "_all");
            response = new ResponseEntity(map, HttpStatus.OK);
        }

        return response;
    }

}
