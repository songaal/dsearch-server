package com.danawa.dsearch.server.dynamic.service;

import com.danawa.dsearch.server.dynamic.adapter.DynamicDbAdapter;
import com.danawa.dsearch.server.dynamic.dto.DynamicInfoResponse;
import com.danawa.dsearch.server.dynamic.entity.DynamicInfo;
import com.danawa.dsearch.server.utils.JsonUtils;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DynamicService {
    private static Logger logger = LoggerFactory.getLogger(DynamicService.class);
    private DynamicDbAdapter dynamicAdapter;
    private String dynamicInfoPath;
    private RestTemplate restTemplate;

    public DynamicService (DynamicDbAdapter dynamicAdapter, @Value("${dsearch.dynamic.path}") String dynamicInfoPath) {
        this.dynamicAdapter = dynamicAdapter;
        this.dynamicInfoPath = dynamicInfoPath;

        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(10 * 1000);
        this.restTemplate = new RestTemplate(factory);
    }

    public List<DynamicInfoResponse> findAll(){
        List<DynamicInfo> dynamicInfoList = dynamicAdapter.findAll();
        if (dynamicInfoList.size() > 0) {
            return dynamicInfoList.stream().map(dynamicInfo -> DynamicInfoResponse.of(dynamicInfo)).collect(Collectors.toList());
        }
        return new ArrayList<DynamicInfoResponse>();
    }

    public Map<String, List<DynamicInfoResponse>> findBundleAll(String desc){
        Map<String, List<DynamicInfoResponse>> result = new HashMap<>();

        List<DynamicInfoResponse> dynamicInfoList = findAll();
        for (DynamicInfoResponse dynamicInfoResponse : dynamicInfoList) {

            if (desc.equals("server")) {
                List<DynamicInfoResponse> subList = result.getOrDefault(dynamicInfoResponse.getBundleServer(), new ArrayList<DynamicInfoResponse>());
                subList.add(dynamicInfoResponse);
                result.put(dynamicInfoResponse.getBundleServer(), subList);
            } else {
                List<DynamicInfoResponse> subList = result.getOrDefault(dynamicInfoResponse.getBundleQueue(), new ArrayList<DynamicInfoResponse>());
                subList.add(dynamicInfoResponse);
                result.put(dynamicInfoResponse.getBundleQueue(), subList);
            }
        }

        return result;
    }

    public int fileUpload(HashMap<String, Object> body) {
        int result = 0;

        try {
            String bodyStr = "";

            if (body.containsKey("dynamic")) {
                bodyStr = String.valueOf(body.get("dynamic"));
            } else {
                return result;
            }

            List<DynamicInfo> bodyList = JsonUtils.createCustomGson().fromJson(bodyStr, new TypeToken<List<DynamicInfo>>(){}.getType());

            if (bodyList.size() > 0) {
                BufferedWriter writer = null;

                try {
                    writer = new BufferedWriter(new FileWriter(new File(dynamicInfoPath)));
                    writer.write(bodyStr);
                    writer.flush();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if(writer != null) writer.close();
                }

                List<DynamicInfo> newReadList = fileRead();

                List<DynamicInfo> readList = new ArrayList<>();

                for (int i=0; i<newReadList.size(); i++) {
                    DynamicInfo dynamicInfo = new DynamicInfo();
                    dynamicInfo.setId(Double.valueOf(String.valueOf(((Map) newReadList.get(i)).get("id"))).longValue());
                    dynamicInfo.setBundleQueue(String.valueOf(((Map) newReadList.get(i)).get("bundleQueue")));
                    dynamicInfo.setBundleServer(String.valueOf(((Map) newReadList.get(i)).get("bundleServer")));
                    dynamicInfo.setScheme(String.valueOf(((Map) newReadList.get(i)).get("scheme")));
                    dynamicInfo.setIp(String.valueOf(((Map) newReadList.get(i)).get("ip")));
                    dynamicInfo.setPort(String.valueOf(((Map) newReadList.get(i)).get("port")));
                    dynamicInfo.setStateEndPoint(String.valueOf(((Map) newReadList.get(i)).get("stateEndPoint")));
                    dynamicInfo.setConsumeEndPoint(String.valueOf(((Map) newReadList.get(i)).get("consumeEndPoint")));
                    readList.add(dynamicInfo);
                }

                if (readList.size() > 0) {
                    List<DynamicInfo> allList = dynamicAdapter.findAll();

                    if (allList.size() > 0) {
                        dynamicAdapter.deleteAll(allList);
                    }

                    result = saveAll(readList).size();
                } else {
                    logger.info("readList size 0");
                }
            } else {
                logger.info("bodyList size 0");
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Exception");
        }

        return result;
    }

    public List<DynamicInfo> saveAll(List<DynamicInfo> list) {
        return dynamicAdapter.saveAll(list);
    }

    public List<DynamicInfo> fileRead() {
        List<DynamicInfo> result = new ArrayList<>();

        try {
            Reader reader = new FileReader(dynamicInfoPath);
            result = JsonUtils.createCustomGson().fromJson(reader, List.class);
        } catch (Exception e) {
            logger.error("", e);
        }

        return result;
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

    public Map<Long, Object> stateAll() {
        Map<Long, Object> result = new HashMap<>();
        List<DynamicInfo> dynamicInfoList = dynamicAdapter.findAll();

        for (DynamicInfo dynamicInfo : dynamicInfoList) {
            result.put(dynamicInfo.getId(), indexerState(dynamicInfo));
        }

        return result;
    }

    public Map<String, Integer> indexerState(DynamicInfo dynamicInfo) {
        Map<String, Integer> result = new HashMap<>();

        try {

            URI url = URI.create(String.format("%s://%s:%s/%s", "http", dynamicInfo.getIp(), dynamicInfo.getPort(), dynamicInfo.getStateEndPoint()));
            ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.GET, new HttpEntity<>(new HashMap<>()), Map.class);
            Map<String, Object> body = responseEntity.getBody();

            int sum = 0;

            if (body.get("consume") != null) {
                Map<String, Object> subMap = (Map) body.get("consume");
                for (String key : subMap.keySet()) {
                    List<String> list = (List) subMap.get(key);
                    result.put(key, list.size());
                    sum += list.size();
                }
                result.put("count", sum);
            } else {
                logger.error("Queue Indexer Not Result {}", dynamicInfo.getIp() + dynamicInfo.getPort() + dynamicInfo.getStateEndPoint());
            }
        } catch (Exception e) {
        }
        return result;
    }

    public int consumeAll(long id, HashMap<String, Object> body) {
        int result = 0;
        DynamicInfo dynamicInfo = dynamicAdapter.findById(id);

        URI url = URI.create(String.format("%s://%s:%s/%s", "http", dynamicInfo.getIp(), dynamicInfo.getPort(), dynamicInfo.getConsumeEndPoint()));
        ResponseEntity<Map> responseEntity = restTemplate.exchange(url, HttpMethod.PUT, new HttpEntity<>(body), Map.class);
        Map<String, Object> stateMap = responseEntity.getBody();

        if (stateMap.get("status") != null) {
            result = (int) stateMap.get("status");
        } else {
            logger.error("Queue Indexer Not Result {}", dynamicInfo.getIp() + dynamicInfo.getPort() + dynamicInfo.getConsumeEndPoint());
        }

        return result;
    }
}
