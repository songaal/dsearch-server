package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.config.ElasticsearchFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Map;

@Service
public class IndexingJobService {

    private static Logger logger = LoggerFactory.getLogger(IndexingJobService.class);
    private final ElasticsearchFactory elasticsearchFactory;

    public IndexingJobService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    /**
     * 소스를 읽어들여 ES 색인에 입력하는 작업.
     * indexer를 외부 프로세스로 실행한다.
     * */
    public void indexing(String index) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("C:\\Program Files\\Amazon Corretto\\jdk1.8.0_252\\bin\\java.exe",
                "-jar", "C:\\Users\\admin\\Downloads\\indexer-0.1.0.jar");

        processBuilder.inheritIO();
        Process process = processBuilder.start();


        //1. status READY 확인.
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(5000); //타임아웃 설정 5초
        factory.setReadTimeout(5000);//타임아웃 설정 5초
        RestTemplate restTemplate = new RestTemplate(factory);
        String uri = "http://localhost:8080/status";
        ResponseEntity<Map> resultMap = restTemplate.exchange(uri, HttpMethod.GET, null, Map.class);
        resultMap.getStatusCodeValue(); //http status code를 확인
        resultMap.getHeaders(); //헤더 정보 확인
        resultMap.getBody(); //실제 데이터 정보 확인
        // READY 이면 시작한다.

        //2. 색인요청
        /*
        * POST http://localhost:8080/start
        * {
            "scheme": "http",
            "host": "es1.danawa.io",
            "port": 80,
            "index": "song5",
            "type": "ndjson",
            "path": "C:\\Projects\\fastcatx-indexer\\src\\test\\resources\\sample.ndjson",
            "encoding": "utf-8",
            "bulkSize": 1000
        }
        * */



        //3. 지속적으로 status 확인
        // SUCCESS | ERROR 인지확인. 최종 문서갯수 확인.
        // ==> 색인정보를 ES의 History와 LAST 인덱스에 입력



        //4. 자식 프로세스 종료
        process.destroy();

    }

    /**
     * 색인 샤드의 TAG 제약을 풀고 전체 클러스터로 확장시킨다.
     * */
    public void propagate() {

    }

    /**
     * 색인을 대표이름으로 alias 하고 노출한다.
     * */
    public void expose() {

    }
}
