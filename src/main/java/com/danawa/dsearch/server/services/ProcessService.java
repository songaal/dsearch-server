package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.entity.Collection;
import com.danawa.dsearch.server.excpetions.IndexingJobFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.yaml.snakeyaml.Yaml;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProcessService {

    private static Logger logger = LoggerFactory.getLogger(ProcessService.class);
    private final RestTemplate restTemplate;

    public ProcessService() {
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        factory.setReadTimeout(10 * 1000);
        restTemplate = new RestTemplate(factory);
    }

    // 전처리 프로세스
    public void preProcess(Collection collection) {
        // 동적색인 off - yaml 파일에서 autoDynamic true 여야 실행됨.
        dynamicIndexSwitch(collection,"off");
    }

    // 후처리 프로세스
    public void postProcess(Collection collection) {
        // 동적색인 on -  yaml 파일에서 autoDynamic true 여야 실행됨.
        dynamicIndexSwitch(collection,"on");
    }

    // 동적색인 스위치
    public void dynamicIndexSwitch (Collection collection, String switchSignal) {
        Collection.Launcher launcher = collection.getLauncher();
        Map<String, Object> yamlSetting = new Yaml().load(launcher.getYaml());

        boolean autoDynamic = (Boolean) yamlSetting.getOrDefault("autoDynamic",false);
        String autoDynamicIndex = collection.getId();
        List<String> autoDynamicQueueNames = Arrays.asList(((String) yamlSetting.getOrDefault("autoDynamicQueueNames","")).split(","));
        String autoDynamicQueueIndexUrl = (String) yamlSetting.getOrDefault("autoDynamicQueueIndexUrl","");
        int autoDynamicQueueIndexConsumeCount = 0;
        if("on".equals(switchSignal.toLowerCase())) {
            try {
                autoDynamicQueueIndexConsumeCount = (int) yamlSetting.getOrDefault("autoDynamicQueueIndexConsumeCount",1);
            } catch (Exception ignore) {
                autoDynamicQueueIndexConsumeCount = Integer.parseInt((String) yamlSetting.getOrDefault("autoDynamicQueueIndexConsumeCount","1"));
            }
        }

        if (autoDynamic) {
            int retryCount = 20;
            while (true) {
                try {
                    // 큐 이름이 여러개 일 경우.
                    if (autoDynamicQueueIndexUrl.split(",").length != 1) {
                        // 멀티 MQ
                        for (int i = 0; i < autoDynamicQueueIndexUrl.split(",").length; i++) {
                            String queueIndexUrl = autoDynamicQueueIndexUrl.split(",")[i];
                            String queueName = autoDynamicQueueNames.get(i);
                            updateQueueIndexerConsume(false, queueIndexUrl, queueName, autoDynamicQueueIndexConsumeCount);
                            Thread.sleep(1000);
                        }
                    } else {
                        // 싱글 MQ
                        for (String autoDynamicQueueName : autoDynamicQueueNames) {
                            updateQueueIndexerConsume(false, autoDynamicQueueIndexUrl, autoDynamicQueueName, autoDynamicQueueIndexConsumeCount);
                            Thread.sleep(1000);
                        }
                    }
                    logger.info("[{}] autoDynamic >>> {} <<<", autoDynamicIndex, switchSignal);
                    break;
                } catch (Exception e) {
                    logger.error("", e);
                    retryCount--;
                    if (retryCount == 0) {
                        logger.warn("max retry!!!!");
                        break;
                    } else {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ignore) {
                        }
                    }
                }
            }
        }
    }

    // 큐인덱서 컨슈머 수정
    public void updateQueueIndexerConsume(boolean dryRun, String queueIndexerUrl, String queueName, int consumeCount) {
        Map<String, Object> body = new HashMap<>();
        body.put("queue", queueName);
        body.put("size", consumeCount);
        logger.info("QueueIndexUrl: {}, queue: {}, count: {}", queueIndexerUrl, queueName, consumeCount);
        if (!dryRun) {
            ResponseEntity<String> response = restTemplate.exchange(queueIndexerUrl,
                    HttpMethod.PUT,
                    new HttpEntity(body),
                    String.class
            );
            logger.info("edit Consume Response: {}", response);
        } else {
            logger.info("[DRY_RUN] queue indexer request skip");
        }
    }
}