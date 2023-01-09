package com.danawa.dsearch.server.jdbc.controller;

import com.danawa.dsearch.server.jdbc.dto.JdbcCreateRequest;
import com.danawa.dsearch.server.jdbc.entity.JdbcInfo;
import com.danawa.dsearch.server.jdbc.service.JdbcService;
import com.danawa.dsearch.server.jdbc.service.JdbcServiceImpl;
import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import org.h2.util.StringUtils;
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
        JdbcInfo info = covertCreateRequestToMap(jdbcCreateRequest);
        response.put("isSuccess", JdbcService.create(clusterId, info));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
    private JdbcInfo covertCreateRequestToMap(JdbcCreateRequest jdbcRequest){
        JdbcInfo jdbcInfo = new JdbcInfo();
        String id = jdbcRequest.getId();
        String name = jdbcRequest.getName();
        String driver = jdbcRequest.getDriver();
        String user = jdbcRequest.getUser();
        String password = jdbcRequest.getPassword();
        String url = jdbcRequest.getUrl();
        String provider = jdbcRequest.getProvider();
        String address = jdbcRequest.getAddress();
        String port = jdbcRequest.getPort();
        String db_name = jdbcRequest.getDB_name();
        String params = jdbcRequest.getParams();

        if(!StringUtils.isNullOrEmpty(id)) jdbcInfo.setId(id);
        if(!StringUtils.isNullOrEmpty(name)) jdbcInfo.setName(name);
        if(!StringUtils.isNullOrEmpty(driver)) jdbcInfo.setDriver(driver);
        if(!StringUtils.isNullOrEmpty(user)) jdbcInfo.setUser(user);
        if(!StringUtils.isNullOrEmpty(password)) jdbcInfo.setPassword(password);
        if(!StringUtils.isNullOrEmpty(url)) jdbcInfo.setUrl(url);
        if(!StringUtils.isNullOrEmpty(provider)) jdbcInfo.setProvider(provider);
        if(!StringUtils.isNullOrEmpty(address)) jdbcInfo.setAddress(address);
        if(!StringUtils.isNullOrEmpty(port)) jdbcInfo.setPort(port);
        if(!StringUtils.isNullOrEmpty(db_name)) jdbcInfo.setDb_name(db_name);
        if(!StringUtils.isNullOrEmpty(params)) jdbcInfo.setParams(params);
        return jdbcInfo;
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
