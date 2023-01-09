package com.danawa.dsearch.server.dynamicIndex.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class QueueIndexerClientTests {

    private QueueIndexerClient queueIndexerClient;

    @Mock
    private RestTemplate restTemplate;

    @BeforeEach
    public void setup() {
        this.queueIndexerClient = new QueueIndexerClient(restTemplate);
    }

    @Test
    @DisplayName("큐인덱서 클라이언트 get 메서드 성공")
    public void get_success(){
        URI url = URI.create(String.format("%s://%s:%s/%s", "http", "127.0.0.1", "8100", "/managements/state"));
        Map<String, Object> result = new HashMap<>();
        result.put("state", "success");

        HttpHeaders header = new HttpHeaders();
        header.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<?> responseEntity = new ResponseEntity<>(
                result,
                header,
                HttpStatus.OK
        );

        given(restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class))
                .willReturn((ResponseEntity<Map>) responseEntity);
        Map<String, Object> response = queueIndexerClient.get(url);
        String responseState = (String)response.get("state");

        Assertions.assertEquals(responseState, (String) result.get("state"));
    }


    @Test
    @DisplayName("큐인덱서 클라이언트 get 메서드 실패 - 연결 실패")
    public void get_connection_fail(){
        URI url = URI.create(String.format("%s://%s:%s/%s", "http", "127.0.0.1", "8100", "/managements/state"));
        Map<String, Object> result = new HashMap<>();
        result.put("state", "fail");

        HttpHeaders header = new HttpHeaders();
        header.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<?> responseEntity = new ResponseEntity<>(
                result,
                header,
                HttpStatus.OK
        );

        given(restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class))
                .willThrow(new RestClientException("연결 실패"));
        Map<String, Object> response = queueIndexerClient.get(url);
        Assertions.assertEquals(0, response.size());
    }

    @Test
    @DisplayName("큐인덱서 클라이언트 put 메서드 성공")
    public void put_success(){
        URI url = URI.create(String.format("%s://%s:%s/%s", "http", "127.0.0.1", "8100", "/managements/consume-all"));
        Map<String, Object> body = new HashMap<>();

        Map<String, Object> result = new HashMap<>();
        result.put("state", "success");

        HttpHeaders header = new HttpHeaders();
        header.setContentType(MediaType.APPLICATION_JSON);

        ResponseEntity<?> responseEntity = new ResponseEntity<>(
                result,
                header,
                HttpStatus.OK
        );

        given(restTemplate.exchange(url, HttpMethod.PUT, new HttpEntity<>(body), Map.class))
                .willReturn((ResponseEntity<Map>) responseEntity);
        Map<String, Object> response = queueIndexerClient.put(url, body);
        String responseState = (String)response.get("state");

        Assertions.assertEquals(responseState, (String) result.get("state"));
    }
}
