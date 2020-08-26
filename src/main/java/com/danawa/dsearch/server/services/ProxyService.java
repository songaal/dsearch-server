package com.danawa.dsearch.server.services;

import org.springframework.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;


@Service
public class ProxyService {
    private static Logger logger = LoggerFactory.getLogger(ProxyService.class);

    public ProxyService( ) {

    }

    public ResponseEntity<byte[]> proxy(String url, String keyword) throws UnsupportedEncodingException {
        ResponseEntity response= null;
        url += URLEncoder.encode(keyword, "UTF-8");
        HttpEntity httpEntity = new HttpEntity<>(url);
        RestTemplate restTemplate = new RestTemplate();
        try {
            response = restTemplate.exchange(new URI(url), HttpMethod.GET, httpEntity, byte[].class);
        } catch (RestClientException | URISyntaxException e) {
            logger.debug("proxy fail : {}", e.getMessage());
        }
        return response;
    }

}
