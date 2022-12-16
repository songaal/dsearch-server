package com.danawa.dsearch.server.dynamic.service;

import com.danawa.dsearch.server.dynamic.adapter.DynamicDbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class DynamicService {
    private static Logger logger = LoggerFactory.getLogger(DynamicService.class);
    private DynamicDbAdapter dynamicAdapter;
    private String dynamicInfoPath;
    private RestTemplate restTemplate;

    public DynamicService (DynamicDbAdapter dynamicAdapter, @Value("${dsearch.dynamic.path}") String dynamicInfoPath, RestTemplate restTemplate) {
        this.dynamicAdapter = dynamicAdapter;
        this.dynamicInfoPath = dynamicInfoPath;
        /*HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        this.restTemplate = new RestTemplate(factory);*/
    }
/*
    public List<DynamicInfo> findAll(){
        return dynamicAdapter.findAll();
    }

    public int fileUpload() {
        List<DynamicInfo> allList = dynamicAdapter.findAll();
        dynamicAdapter.deleteAll(allList);
        List<DynamicInfo> result = dynamicAdapter.saveAll(fileRead());

        return result.size();
    }

    public List<DynamicInfo> fileRead() {
        try {
            List<DynamicInfo> result = new ArrayList<>();

            Reader reader = new FileReader(dynamicInfoPath);
            List list = JsonUtils.createCustomGson().fromJson(reader, List.class);

            for (int i=0; i<list.size(); i++) {
                DynamicInfo dynamicInfo = new DynamicInfo();
                dynamicInfo.setBundle(String.valueOf(((Map) list.get(i)).get("bundle")));
                dynamicInfo.setIp(String.valueOf(((Map) list.get(i)).get("ip")));
                dynamicInfo.setPort((Integer) ((Map) list.get(i)).get("port"));
                dynamicInfo.setEndPoint(String.valueOf(((Map) list.get(i)).get("endPoint")));
                result.add(dynamicInfo);
            }

            return result;
        } catch (Exception e) {
            logger.error("", e);
        }
        return new ArrayList<>();
    }

    public Map<String, Integer> state(long id) {
        Map<String, Integer> result = new HashMap<>();
        DynamicInfo dynamicInfo = dynamicAdapter.findById(id);

        if (dynamicInfo != null) {
            return indexerState(dynamicInfo);
        } else {
            logger.info("Not Found Id {}", id);
        }

        return result;
    }

    public Map<String, Integer> indexerState(DynamicInfo dynamicInfo) {
        Map<String, Integer> result = new HashMap<>();

        URI url = URI.create(String.format("%s://%s:%d/%s", "http", dynamicInfo.getIp(), dynamicInfo.getPort(), dynamicInfo.getEndPoint()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
        Map<String, Object> body = responseEntity.getBody();

        if (body.get("consume") != null) {
            Map<String, Object> subMap = (Map) body.get("consume");
            for (String key : subMap.keySet()) {
                List<String> list = (List) subMap.get(key);
                result.put(key, list.size());
            }
        } else {
            logger.error("Queue Indexer Not Data {}", dynamicInfo.getIp() + dynamicInfo.getPort());
        }
        return result;
    }*/
}
