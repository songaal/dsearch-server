package com.danawa.dsearch.server.clusters.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.clusters.dto.NodeMoveInfoResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class NodeService {
    private static Logger logger = LoggerFactory.getLogger(NodeService.class);

    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    public NodeService(ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper) {
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    public List<NodeMoveInfoResponse> getNodeMoveInfo(UUID clusterId) {
        List<NodeMoveInfoResponse> result = new ArrayList<>();

        try{
            List<Map<String, Object>> moveInfoList = elasticsearchFactoryHighLevelWrapper.getClusterShardSettings(clusterId);

            HashSet<String> indexes = new HashSet<>();
            for(Map<String, Object> moveInfo: moveInfoList){
                String indexState = (String) moveInfo.get("state");
                String indexName = (String) moveInfo.get("index");
                if (!indexState.equals("STARTED")) {
                    indexes.add(indexName);
                }
            }

            if (indexes.size() != 0) {
                List<Map<String, Object>> indexMoveInfoEntityList =
                        elasticsearchFactoryHighLevelWrapper.getClusterShardDetailSettings(clusterId, String.join(",", indexes));
                result = changeToMoveInfoResponse(indexMoveInfoEntityList);
            }

        } catch (IOException e) {
            logger.error("", e);
        }
        return result;
    }

    public List<NodeMoveInfoResponse> changeToMoveInfoResponse(List<Map<String, Object>> moveInfoList) {
        List<NodeMoveInfoResponse> response = new ArrayList<>();

        for(Map<String, Object> moveInfo: moveInfoList ){
            String currentStage = (String) moveInfo.get("stage");
            if (!currentStage.equals("done")) {
                NodeMoveInfoResponse nodeMoveInfoResponse = new NodeMoveInfoResponse();

                nodeMoveInfoResponse.setIndex((String) moveInfo.get("index"));
                nodeMoveInfoResponse.setShard((String) moveInfo.get("shard"));
                nodeMoveInfoResponse.setTime((String) moveInfo.get("time"));
                nodeMoveInfoResponse.setType((String) moveInfo.get("type"));
                nodeMoveInfoResponse.setStage((String) moveInfo.get("stage"));
                nodeMoveInfoResponse.setSourceNode((String) moveInfo.get("source_node"));
                nodeMoveInfoResponse.setTargetNode((String) moveInfo.get("target_node"));
                nodeMoveInfoResponse.setFiles((String) moveInfo.get("files"));
                nodeMoveInfoResponse.setFilesRecovered((String) moveInfo.get("files_recovered"));
                nodeMoveInfoResponse.setFilesPercent((String) moveInfo.get("files_percent"));
                nodeMoveInfoResponse.setBytes((String) moveInfo.get("bytes"));
                nodeMoveInfoResponse.setBytesRecovered((String) moveInfo.get("bytes_recovered"));
                nodeMoveInfoResponse.setBytesPercent((String) moveInfo.get("bytes_percent"));
                nodeMoveInfoResponse.setBytesTotal((String) moveInfo.get("bytes_total"));
                nodeMoveInfoResponse.setTranslogOps((String) moveInfo.get("translog_ops"));
                nodeMoveInfoResponse.setTranslogOpsRecovered((String) moveInfo.get("translog_ops_recovered"));
                nodeMoveInfoResponse.setTranslogOpsPercent((String) moveInfo.get("translog_ops_percent"));
                response.add(nodeMoveInfoResponse);
            }
        }
        return response;
    }

    // 제외된 노드 이름 가져오기
    public List<String> getExcludedNodes(UUID clusterId) {
        ArrayList<String> result = new ArrayList<>();

        try{
            Map<String, Object> indexEntityList = elasticsearchFactoryHighLevelWrapper.getClusterSettings(clusterId);
            Map<String, String> trainsientMap = ((Map<String, String>) indexEntityList.get("transient"));
            for (String key : trainsientMap.keySet()) {
                if (key.equals("cluster.routing.allocation.exclude._name")) {
                    for (String node : trainsientMap.get(key).split(",")) {
                        result.add(node);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("", e);
        }

        return result;
    }

    // 노드 제거
    public String removeNode(UUID clusterId, String body) {
        String result = "";
        try{
            result = elasticsearchFactoryHighLevelWrapper.removeNode(clusterId, body);
        } catch (IOException e) {
            logger.error("", e);
        }
        return result;
    }
}
