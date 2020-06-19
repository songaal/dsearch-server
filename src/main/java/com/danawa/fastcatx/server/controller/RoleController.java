package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.Role;
import com.danawa.fastcatx.server.services.RoleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/roles")
public class RoleController {
    private static Logger logger = LoggerFactory.getLogger(RoleController.class);

    private final RoleService roleService;

    public RoleController(RoleService roleService) {
        this.roleService = roleService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable long id) {
        return new ResponseEntity<>(roleService.find(id), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody Role role) {
        return new ResponseEntity<>(roleService.add(role), HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable long id,
                                  @RequestBody Role role) {
        return new ResponseEntity<>(roleService.edit(id, role), HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable long id) {
        return new ResponseEntity<>(roleService.remove(id), HttpStatus.OK);
    }

}
