package com.danawa.dsearch.server.node.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactoryHighLevelWrapper;
import com.danawa.dsearch.server.node.dto.NodeMoveInfoResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class NodeService {
    private static Logger logger = LoggerFactory.getLogger(NodeService.class);

    private ElasticsearchFactory elasticsearchFactory;
    private ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper;

    public NodeService(ElasticsearchFactory elasticsearchFactory,
                       ElasticsearchFactoryHighLevelWrapper elasticsearchFactoryHighLevelWrapper) {
        this.elasticsearchFactory = elasticsearchFactory;
        this.elasticsearchFactoryHighLevelWrapper = elasticsearchFactoryHighLevelWrapper;
    }

    public List<NodeMoveInfoResponse> moveInfo(UUID clusterId) throws IOException {
        List<NodeMoveInfoResponse> result = new ArrayList<>();

        try{
            List indexEntityList = elasticsearchFactoryHighLevelWrapper.getClusterShardSettings(clusterId);

            HashSet<String> indexes = new HashSet<>();
            for (int i = 0; i < indexEntityList.size(); i++) {
                if (!String.valueOf(((Map) indexEntityList.get(i)).get("state")).equals("STARTED")) {
                    indexes.add(String.valueOf(((Map) indexEntityList.get(i)).get("index")));
                }
            }

            if (indexes.size() != 0) {
                List indexMoveInfoEntityList = elasticsearchFactoryHighLevelWrapper.getClusterShardDetailSettings(clusterId, String.join(",", indexes));
                result = moveResponseSetting(indexMoveInfoEntityList);
            }

        } catch (IOException e) {
            logger.error("", e);
        }
        return result;
    }

    public List<NodeMoveInfoResponse> moveResponseSetting(List indexMoveInfoEntityList) {
        List<NodeMoveInfoResponse> moveInfoList = new ArrayList<>();

        for (int i = 0; i < indexMoveInfoEntityList.size(); i++) {
            if (!String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("stage")).equals("done")) {
                NodeMoveInfoResponse nodeMoveInfoResponse = new NodeMoveInfoResponse();
                nodeMoveInfoResponse.setIndex(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("index")));
                nodeMoveInfoResponse.setShard(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("shard")));
                nodeMoveInfoResponse.setTime(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("time")));
                nodeMoveInfoResponse.setType(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("type")));
                nodeMoveInfoResponse.setStage(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("stage")));
                nodeMoveInfoResponse.setSourceNode(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("source_node")));
                nodeMoveInfoResponse.setTargetNode(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("target_node")));
                nodeMoveInfoResponse.setFiles(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("files")));
                nodeMoveInfoResponse.setFilesRecovered(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("files_recovered")));
                nodeMoveInfoResponse.setFilesPercent(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("files_percent")));
                nodeMoveInfoResponse.setBytes(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("bytes")));
                nodeMoveInfoResponse.setBytesRecovered(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("bytes_recovered")));
                nodeMoveInfoResponse.setBytesPercent(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("bytes_percent")));
                nodeMoveInfoResponse.setBytesTotal(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("bytes_total")));
                nodeMoveInfoResponse.setTranslogOps(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("translog_ops")));
                nodeMoveInfoResponse.setTranslogOpsRecovered(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("translog_ops_recovered")));
                nodeMoveInfoResponse.setTranslogOpsPercent(String.valueOf(((Map) indexMoveInfoEntityList.get(i)).get("translog_ops_percent")));
                moveInfoList.add(nodeMoveInfoResponse);
            }
        }
        return moveInfoList;
    }

    // 제외 샤드 체크
    public List<String> nodeClusterInfo(UUID clusterId) {
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
    public String removeNode(UUID clusterId, String body) throws IOException {
        String result = "";
        try{
            result = elasticsearchFactoryHighLevelWrapper.removeNode(clusterId, body);
        } catch (IOException e) {
            logger.error("", e);
        }
        return result;
    }
}
