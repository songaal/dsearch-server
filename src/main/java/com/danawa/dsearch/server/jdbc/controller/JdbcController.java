package com.danawa.dsearch.server.jdbc.controller;

import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
import com.danawa.dsearch.server.jdbc.service.JdbcServiceImpl;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;


@RestController
@RequestMapping("/jdbc")
public class JdbcController {
    private final JdbcService JdbcService;

    private JdbcController(JdbcService JdbcService) {
        this.JdbcService = JdbcService;
    }

    @PostMapping("/")
    public ResponseEntity<?> isConnectable(@RequestBody JdbcUpdateRequest jdbcRequest){
        Map<String, Object> response = new HashMap<String, Object>();
        response.put("message", JdbcService.isConnectable(jdbcRequest));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/list")
    public ResponseEntity<?> findAll(@RequestHeader(value = "cluster-id") UUID clusterId) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("list", JdbcService.findAll(clusterId));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PutMapping("/add")
    public ResponseEntity<?> create(@RequestHeader(value = "cluster-id") UUID clusterId,
                                           @RequestBody JdbcCreateRequest jdbcCreateRequest) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", JdbcService.create(clusterId, jdbcCreateRequest));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> delete(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @PathVariable String id) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", JdbcService.delete(clusterId, id));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/update/{id}")
    public ResponseEntity<?> update(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody JdbcUpdateRequest jdbcUpdateRequest,
                                              @PathVariable String id) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", JdbcService.update(clusterId, id, jdbcUpdateRequest));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
