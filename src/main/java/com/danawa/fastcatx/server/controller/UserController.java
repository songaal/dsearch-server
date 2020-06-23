package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.UpdateUserRequest;
import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.excpetions.NotFoundException;
import com.danawa.fastcatx.server.services.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/users")
public class UserController {
    private static Logger logger = LoggerFactory.getLogger(UserController.class);

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping
    public ResponseEntity<?> findAll() {
        return new ResponseEntity<>(userService.findAll(), HttpStatus.OK);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> find(@PathVariable long id) {
        return new ResponseEntity<>(userService.find(id), HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody User user) {
        return new ResponseEntity<>(userService.add(user), HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable Long id) {
        return new ResponseEntity<>(userService.remove(id), HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable Long id,
                                  @RequestParam String action,
                                  @RequestBody UpdateUserRequest updateUser) throws NotFoundException {
        User registerUser = null;
        if ("UPDATE_PASSWORD".equalsIgnoreCase(action)) {
            registerUser = userService.editPassword(id, updateUser);
        }
        return new ResponseEntity<>(registerUser, HttpStatus.OK);
    }

}
