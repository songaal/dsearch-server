package com.danawa.dsearch.server.controller;

import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@RestController
@RequestMapping("/kibana")
public class KibanaController {

    @PostMapping("/status")
    public ResponseEntity<?> getKibanaStatus(@RequestBody Map<String, Object> url) {

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setConnectionRequestTimeout(5000);
        RestTemplate template = new RestTemplate(factory);

        HttpHeaders headers = new HttpHeaders();
        HttpEntity<?> entity = new HttpEntity<>(headers);

        ResponseEntity responseEntity = template.exchange((String) url.get("url"), HttpMethod.GET, entity, Map.class);
        return new ResponseEntity<>(responseEntity.getBody(), HttpStatus.OK);
    }
}
