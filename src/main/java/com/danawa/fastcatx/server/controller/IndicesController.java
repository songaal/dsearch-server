package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.services.IndicesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@CrossOrigin("*")
@RequestMapping("/indices")
public class IndicesController {
    private static Logger logger = LoggerFactory.getLogger(IndicesController.class);

    private IndicesService indicesService;

    public IndicesController(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @GetMapping("/{indices}/_docs")
    public ResponseEntity<?> findAllDocument(@PathVariable String indices,
                                             @RequestParam(defaultValue = "0") int page,
                                             @RequestParam(defaultValue = "100") int size) throws IOException {

        return new ResponseEntity<>(indicesService.findAllDocument(indices, page, size), HttpStatus.OK);
    }


}
