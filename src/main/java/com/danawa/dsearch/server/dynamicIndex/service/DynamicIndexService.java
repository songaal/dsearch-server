package com.danawa.dsearch.server.dynamicIndex.service;

import com.danawa.dsearch.server.dynamicIndex.adapter.DynamicIndexAdapter;
import com.danawa.dsearch.server.dynamicIndex.adapter.DynamicIndexDatabaseAdapter;
import com.danawa.dsearch.server.dynamicIndex.dto.DynamicIndexInfoResponse;
import com.danawa.dsearch.server.dynamicIndex.entity.BundleDescription;
import com.danawa.dsearch.server.dynamicIndex.entity.DynamicIndexInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DynamicIndexService {
    private static Logger logger = LoggerFactory.getLogger(DynamicIndexService.class);

    private DynamicIndexAdapter adapter;

    private QueueIndexerClient queueIndexerClient;

    public DynamicIndexService(DynamicIndexAdapter adapter,
                               QueueIndexerClient queueIndexerClient) {
        this.adapter = adapter;
        this.queueIndexerClient = queueIndexerClient;
    }

    public List<DynamicIndexInfoResponse> findAll(){
        List<DynamicIndexInfo> dynamicIndexInfoList = adapter.findAll();

        if (dynamicIndexInfoList.size() > 0) {
            return dynamicIndexInfoList.stream().map(dynamicInfo -> DynamicIndexInfoResponse.of(dynamicInfo)).collect(Collectors.toList());
        }

        return new ArrayList<DynamicIndexInfoResponse>();
    }

    public Map<String, List<DynamicIndexInfoResponse>> findBundleAll(BundleDescription descriptionType){
        Map<String, List<DynamicIndexInfoResponse>> result = new HashMap<>();

        List<DynamicIndexInfoResponse> dynamicInfoList = findAll();
        for (DynamicIndexInfoResponse dynamicIndexInfoResponse : dynamicInfoList) {

            if (descriptionType == BundleDescription.SERVER) {
                // 큐 인덱서에 대한 서버 별 묶음 정렬
                String bundleServer = dynamicIndexInfoResponse.getBundleServer();
                List<DynamicIndexInfoResponse> subList = result.getOrDefault(bundleServer, new ArrayList<>());
                subList.add(dynamicIndexInfoResponse);
                result.put(bundleServer, subList);

            } else if(descriptionType == BundleDescription.QUEUE){
                // 큐 인덱서에 대한 큐 단위 묶음 정렬
                String bundleQueue = dynamicIndexInfoResponse.getBundleQueue();
                List<DynamicIndexInfoResponse> subList = result.getOrDefault(bundleQueue, new ArrayList<>());
                subList.add(dynamicIndexInfoResponse);
                result.put(bundleQueue, subList);
            }
        }

        return result;
    }

//    public int fileUpload(HashMap<String, Object> body) {
//        int result = 0;
//
//        try {
//            String bodyStr = "";
//
//            if (body.containsKey("dynamic")) {
//                bodyStr = String.valueOf(body.get("dynamic"));
//            } else {
//                return result;
//            }
//
//            List<DynamicIndexInfo> bodyList = JsonUtils.createCustomGson().fromJson(bodyStr, new TypeToken<List<DynamicIndexInfo>>(){}.getType());
//
//            if (bodyList.size() > 0) {
//                BufferedWriter writer = null;
//
//                try {
//                    writer = new BufferedWriter(new FileWriter(new File(dynamicInfoPath)));
//                    writer.write(bodyStr);
//                    writer.flush();
//                } catch (FileNotFoundException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } finally {
//                    if(writer != null) writer.close();
//                }
//
//                List<DynamicIndexInfo> newReadList = fileRead();
//
//                List<DynamicIndexInfo> readList = new ArrayList<>();
//
//                for (int i=0; i<newReadList.size(); i++) {
//                    DynamicIndexInfo dynamicIndexInfo = new DynamicIndexInfo();
//                    dynamicIndexInfo.setId(Double.valueOf(String.valueOf(((Map) newReadList.get(i)).get("id"))).longValue());
//                    dynamicIndexInfo.setBundleQueue(String.valueOf(((Map) newReadList.get(i)).get("bundleQueue")));
//                    dynamicIndexInfo.setBundleServer(String.valueOf(((Map) newReadList.get(i)).get("bundleServer")));
//                    dynamicIndexInfo.setScheme(String.valueOf(((Map) newReadList.get(i)).get("scheme")));
//                    dynamicIndexInfo.setIp(String.valueOf(((Map) newReadList.get(i)).get("ip")));
//                    dynamicIndexInfo.setPort(String.valueOf(((Map) newReadList.get(i)).get("port")));
//                    dynamicIndexInfo.setStateEndPoint(String.valueOf(((Map) newReadList.get(i)).get("stateEndPoint")));
//                    dynamicIndexInfo.setConsumeEndPoint(String.valueOf(((Map) newReadList.get(i)).get("consumeEndPoint")));
//                    readList.add(dynamicIndexInfo);
//                }
//
//                if (readList.size() > 0) {
//                    List<DynamicIndexInfo> allList = dynamicAdapter.findAll();
//
//                    if (allList.size() > 0) {
//                        dynamicAdapter.deleteAll(allList);
//                    }
//
//                    result = saveAll(readList).size();
//                } else {
//                    logger.info("readList size 0");
//                }
//            } else {
//                logger.info("bodyList size 0");
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.info("Exception");
//        }
//
//        return result;
//    }

    public List<DynamicIndexInfo> saveAll(List<DynamicIndexInfo> list) {
        return adapter.saveAll(list);
    }

//    public List<DynamicIndexInfo> fileRead() {
//        List<DynamicIndexInfo> result = new ArrayList<>();
//
//        try {
//            Reader reader = new FileReader(dynamicInfoPath);
//            result = JsonUtils.createCustomGson().fromJson(reader, List.class);
//        } catch (Exception e) {
//            logger.error("", e);
//        }
//
//        return result;
//    }

    public Map<String, Integer> state(long id) {
        Map<String, Integer> result = new HashMap<>();
        DynamicIndexInfo dynamicIndexInfo = adapter.findById(id);

        if (dynamicIndexInfo != null) {
            return indexerState(dynamicIndexInfo);
        } else {
            logger.info("Not Found Id {}", id);
        }

        return result;
    }

    public Map<Long, Object> stateAll() {
        Map<Long, Object> result = new HashMap<>();
        List<DynamicIndexInfo> dynamicIndexInfoList = adapter.findAll();

        for (DynamicIndexInfo dynamicIndexInfo : dynamicIndexInfoList) {
            result.put(dynamicIndexInfo.getId(), indexerState(dynamicIndexInfo));
        }

        return result;
    }

    public Map<String, Integer> indexerState(DynamicIndexInfo dynamicIndexInfo) {
        Map<String, Integer> result = new HashMap<>();

        try {

            URI url = URI.create(String.format("%s://%s:%s/%s", "http", dynamicIndexInfo.getIp(), dynamicIndexInfo.getPort(), dynamicIndexInfo.getStateEndPoint()));
            ResponseEntity<?> responseEntity = queueIndexerClient.get(url);
            Map<String, Object> body = (Map<String, Object>) responseEntity.getBody();

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
                logger.error("Queue Indexer Not Result {}", dynamicIndexInfo.getIp() + dynamicIndexInfo.getPort() + dynamicIndexInfo.getStateEndPoint());
            }
        } catch (Exception e) {
        }
        return result;
    }

    public int consumeAll(long id, HashMap<String, Object> body) {
        int result = 0;
        DynamicIndexInfo dynamicIndexInfo = adapter.findById(id);

        URI url = URI.create(String.format("%s://%s:%s/%s", "http", dynamicIndexInfo.getIp(), dynamicIndexInfo.getPort(), dynamicIndexInfo.getConsumeEndPoint()));
        ResponseEntity<?> responseEntity = queueIndexerClient.put(url, body);
        Map<String, Object> stateMap = (Map<String, Object>) responseEntity.getBody();

        if (stateMap.get("status") != null) {
            result = (int) stateMap.get("status");
        } else {
            logger.error("Queue Indexer Not Result {}", dynamicIndexInfo.getIp() + dynamicIndexInfo.getPort() + dynamicIndexInfo.getConsumeEndPoint());
        }

        return result;
    }
}
