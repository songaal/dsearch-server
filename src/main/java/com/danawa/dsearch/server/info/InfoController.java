package com.danawa.dsearch.server.info;

import org.springframework.beans.factory.annotation.Value;
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
    @Value("${dsearch.build.version}")
    private String version;
    @Value("${dsearch.build.name}")
    private String name;

    @GetMapping
    public ResponseEntity<?> info() {
        Map<String, String> info = new HashMap<>();
        info.put("name", name);
        info.put("version", version);
        return new ResponseEntity<>(info, HttpStatus.OK);
    }

}
