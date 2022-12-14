package com.danawa.dsearch.server.node.service;

import com.danawa.dsearch.server.elasticsearch.ElasticsearchFactory;
import com.danawa.dsearch.server.node.dto.NodeMoveInfoResponse;
import com.google.gson.Gson;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class NodeService {
    private static Logger logger = LoggerFactory.getLogger(NodeService.class);

    private ElasticsearchFactory elasticsearchFactory;

    public NodeService(ElasticsearchFactory elasticsearchFactory) {
        this.elasticsearchFactory = elasticsearchFactory;
    }

    // 샤드 이동 체크
    public List<NodeMoveInfoResponse> moveInfo(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            //1. _cat/shards?h=index,state&format=json
            Request nodeIndexRequest = new Request("GET", "/_cat/shards");
            nodeIndexRequest.addParameter("h", "index,state");
            nodeIndexRequest.addParameter("format", "json");
            Response indicesResponse = client.getLowLevelClient().performRequest(nodeIndexRequest);

            List indexEntityList = new Gson().fromJson(EntityUtils.toString(indicesResponse.getEntity()), List.class);

            HashSet<String> indexes = new HashSet<>();
            for (int i = 0; i < indexEntityList.size(); i++) {
                if (!String.valueOf(((Map) indexEntityList.get(i)).get("state")).equals("STARTED")) {
                    indexes.add(String.valueOf(((Map) indexEntityList.get(i)).get("index")));
                }
            }

            List<NodeMoveInfoResponse> moveInfoList = new ArrayList<>();

            if (indexes.size() != 0) {
                //2. _cat/recovery/ntour,live_shopping?format=json
                Request nodeIndexDetailRequest = new Request("GET", "/_cat/recovery/" + String.join(",", indexes));
                nodeIndexDetailRequest.addParameter("format", "json");
                Response indicesDetailResponse = client.getLowLevelClient().performRequest(nodeIndexDetailRequest);

                List indexMoveInfoEntityList = new Gson().fromJson(EntityUtils.toString(indicesDetailResponse.getEntity()), List.class);
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
            }

            return moveInfoList;
        }
    }

    // 제외 샤드 체크
    public List<String> nodeClusterInfo(UUID clusterId) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request request = new Request("GET", "/_cluster/settings");
            request.addParameter("flat_settings", "true");
            request.addParameter("filter_path", "transient");
            Response response = client.getLowLevelClient().performRequest(request);

            Map<String, Map<String, String>> indexEntityList = new Gson().fromJson(EntityUtils.toString(response.getEntity()), Map.class);

            for (String key : indexEntityList.get("transient").keySet()) {
                if (key.equals("cluster.routing.allocation.exclude._name")) {
                    return new ArrayList<String>(Arrays.asList(indexEntityList.get("transient").get(key).split(",")));
                }
            }

            return new ArrayList<String>();
        }
    }

    // 노드 제거
    public String removeNode(UUID clusterId, String body) throws IOException {
        try (RestHighLevelClient client = elasticsearchFactory.getClient(clusterId)) {
            Request removeNodeRequest = new Request("PUT", "/_cluster/settings");
            removeNodeRequest.setJsonEntity(body);
            Response removeNodeResponse = client.getLowLevelClient().performRequest(removeNodeRequest);
            return EntityUtils.toString(removeNodeResponse.getEntity());
        }
    }
}
