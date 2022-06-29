package com.danawa.dsearch.server.jdbc.controller;

import com.danawa.dsearch.server.jdbc.service.JdbcService;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;


@RestController
@RequestMapping("/jdbc")
public class JdbcController {
    private final JdbcService jdbcService;

    private JdbcController(JdbcService jdbcService) {
        this.jdbcService = jdbcService;
    }

    @PostMapping("/")
    public ResponseEntity<?> getConnectionTest(@RequestBody JdbcRequest jdbcRequest) throws Exception {
        System.out.println(jdbcRequest.getUrl());
        System.out.println(jdbcRequest.getDriver());
        Map<String, Object> response = new HashMap<String, Object>();
        boolean flag = jdbcService.connectionTest(jdbcRequest);
        response.put("message", flag);
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/list")
    public ResponseEntity<?> getJdbcList(@RequestHeader(value = "cluster-id") UUID clusterId) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("list", jdbcService.getJdbcList(clusterId));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PutMapping("/add")
    public ResponseEntity<?> addJdbcSource(@RequestHeader(value = "cluster-id") UUID clusterId,
                                           @RequestBody JdbcRequest jdbcRequest) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", jdbcService.addJdbcSource(clusterId, jdbcRequest));
        return new ResponseEntity<>(jdbcService.addJdbcSource(clusterId, jdbcRequest), HttpStatus.OK);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> deleteJdbcSource(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @PathVariable String id) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", jdbcService.deleteJdbcSource(clusterId, id));
        System.out.println(response.get("isSuccess"));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/update/{id}")
    public ResponseEntity<?> updateJdbcSource(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody JdbcRequest jdbcRequest,
                                              @PathVariable String id) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", jdbcService.updateJdbcSource(clusterId, id, jdbcRequest));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
