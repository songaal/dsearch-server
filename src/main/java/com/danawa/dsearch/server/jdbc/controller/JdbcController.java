package com.danawa.dsearch.server.jdbc.controller;

import com.danawa.dsearch.server.jdbc.service.JdbcServiceImpl;
import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;


@RestController
@RequestMapping("/jdbc")
public class JdbcController {
    private final JdbcServiceImpl jdbcServiceImpl;

    private JdbcController(JdbcServiceImpl jdbcServiceImpl) {
        this.jdbcServiceImpl = jdbcServiceImpl;
    }

    @PostMapping("/")
    public ResponseEntity<?> isConnectable(@RequestBody JdbcRequest jdbcRequest){
        Map<String, Object> response = new HashMap<String, Object>();
        response.put("message", jdbcServiceImpl.isConnectable(jdbcRequest));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/list")
    public ResponseEntity<?> findAll(@RequestHeader(value = "cluster-id") UUID clusterId) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("list", jdbcServiceImpl.findAll(clusterId));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PutMapping("/add")
    public ResponseEntity<?> create(@RequestHeader(value = "cluster-id") UUID clusterId,
                                           @RequestBody JdbcRequest jdbcRequest) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", jdbcServiceImpl.create(clusterId, jdbcRequest));
        return new ResponseEntity<>(jdbcServiceImpl.create(clusterId, jdbcRequest), HttpStatus.OK);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> delete(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @PathVariable String id) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", jdbcServiceImpl.delete(clusterId, id));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @PostMapping("/update/{id}")
    public ResponseEntity<?> update(@RequestHeader(value = "cluster-id") UUID clusterId,
                                              @RequestBody JdbcRequest jdbcRequest,
                                              @PathVariable String id) throws Exception {
        Map<String, Object> response = new HashMap<>();
        response.put("isSuccess", jdbcServiceImpl.update(clusterId, id, jdbcRequest));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
