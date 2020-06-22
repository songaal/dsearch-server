package com.danawa.fastcatx.server.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/info")
public class InfoController {

    @Value("${fastcatx.build.version}")
    private String version;
    @Value("${fastcatx.build.name}")
    private String name;

    @GetMapping
    public ResponseEntity<?> build() {
        Map<String, String> build = new HashMap<>();
        build.put("name", name);
        build.put("version", version);
        return new ResponseEntity<>(build, HttpStatus.OK);
    }

}
