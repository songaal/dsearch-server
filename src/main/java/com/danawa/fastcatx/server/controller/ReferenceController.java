package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.services.ReferenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin("*")
@RequestMapping("/reference")
public class ReferenceController {
    private static Logger logger = LoggerFactory.getLogger(ReferenceController.class);

    private ReferenceService referenceService;

    public ReferenceController(ReferenceService referenceService) {
        this.referenceService = referenceService;
    }

    @GetMapping
    public ResponseEntity<?> findAll() {
        referenceService.findAll();
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable String id) {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable String id) {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable String id) {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/_search")
    public ResponseEntity<?> search() {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/{id}/_search")
    public ResponseEntity<?> search(@PathVariable String id) {
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
