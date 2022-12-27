package com.danawa.dsearch.server.clusters.controller;

import com.danawa.dsearch.server.clusters.dto.NodeMoveInfoResponse;
import com.danawa.dsearch.server.clusters.service.NodeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/node")
public class NodeController {
    private static Logger logger = LoggerFactory.getLogger(NodeController.class);

    private NodeService nodeService;
    public NodeController(NodeService nodeService) {
        this.nodeService = nodeService;
    }

    @GetMapping("/move/info")
    public ResponseEntity<?> moveInfo(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        List<NodeMoveInfoResponse> response = nodeService.getMoveInfo(clusterId);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/_cluster/settings")
    public ResponseEntity<?> nodeClusterInfo(@RequestHeader(value = "cluster-id") UUID clusterId) throws IOException {
        List<String> nodeClusterInfoList = nodeService.nodeClusterInfo(clusterId);
        return new ResponseEntity<>(nodeClusterInfoList, HttpStatus.OK);
    }

    @PutMapping("/_cluster/remove")
    public ResponseEntity<?> removeNode(@RequestHeader(value = "cluster-id") UUID clusterId,
                                            @RequestBody HashMap<String, Object> body) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String response = nodeService.removeNode(clusterId, mapper.writeValueAsString(body));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
