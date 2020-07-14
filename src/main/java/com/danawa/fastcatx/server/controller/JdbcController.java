package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.JdbcRequest;
import com.danawa.fastcatx.server.services.JdbcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.*;


@RestController
@RequestMapping("/jdbc")
public class JdbcController {
    private static Logger logger = LoggerFactory.getLogger(JdbcController.class);

    @Autowired
    private JdbcService jdbcService;

    public JdbcController() { }

    @PostMapping("/")
    public ResponseEntity<?> getConnectionTest(@RequestBody JdbcRequest jdbcRequest) throws Exception {
        Map<String, Object> resultEntitiy = new HashMap<String, Object>();
        boolean flag = jdbcService.connectionTest(jdbcRequest);
        resultEntitiy.put("message", flag);
        return new ResponseEntity<>(resultEntitiy, HttpStatus.OK);
    }
}
