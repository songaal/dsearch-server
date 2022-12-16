package com.danawa.dsearch.server.dynamic.controller;

import com.danawa.dsearch.server.dynamic.service.DynamicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/dynamic")
public class DynamicController {
    private static Logger logger = LoggerFactory.getLogger(DynamicController.class);

    private DynamicService dynamicService;

    public DynamicController(DynamicService dynamicService) {
        this.dynamicService = dynamicService;
    }
/*
    @GetMapping("/find")
    public ResponseEntity<?> getFindAll() {
        return new ResponseEntity<>(dynamicService.findAll(), HttpStatus.OK);
    }

    @GetMapping("/state/{id}")
    public ResponseEntity<?> getState(@PathVariable long id) {
        return new ResponseEntity<>(dynamicService.state(id), HttpStatus.OK);
    }

    @GetMapping("/upload")
    public ResponseEntity<?> getFileUpload() {
        return new ResponseEntity<>(dynamicService.fileUpload(), HttpStatus.OK);
    }*/
}
