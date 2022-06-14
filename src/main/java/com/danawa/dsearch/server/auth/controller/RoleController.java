package com.danawa.dsearch.server.auth.controller;

import com.danawa.dsearch.server.auth.service.RoleService;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import com.danawa.dsearch.server.auth.entity.Role;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/roles")
public class RoleController {
    private static Logger logger = LoggerFactory.getLogger(RoleController.class);
    private final RoleService roleService;
    private final UserRolesService userRolesService;

    public RoleController(RoleService roleService, UserRolesService userRolesService) {
        this.roleService = roleService;
        this.userRolesService = userRolesService;
    }

    @GetMapping
    public ResponseEntity<?> findAll() {
        Map<String, Object> response = new HashMap<>();
        response.put("roles", roleService.findAll());
        response.put("userRoles", userRolesService.findAll());
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable long id) throws NotFoundUserException {
        return new ResponseEntity<>(roleService.find(id), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody Role role) {
        return new ResponseEntity<>(roleService.add(role), HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable long id,
                                  @RequestBody Role role) throws NotFoundUserException {
        return new ResponseEntity<>(roleService.edit(id, role), HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable long id) throws NotFoundUserException {
        return new ResponseEntity<>(roleService.remove(id), HttpStatus.OK);
    }

}
