package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.AddUserRequest;
import com.danawa.fastcatx.server.entity.UpdateUserRequest;
import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.entity.UserRoles;
import com.danawa.fastcatx.server.excpetions.DuplicateException;
import com.danawa.fastcatx.server.excpetions.NotFoundUserException;
import com.danawa.fastcatx.server.services.UserRolesService;
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
    private final UserRolesService userRolesService;

    public UserController(UserService userService, UserRolesService userRolesService) {
        this.userService = userService;
        this.userRolesService = userRolesService;
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
    public ResponseEntity<?> add(@RequestBody AddUserRequest addUserRequest) throws DuplicateException {
        User registerUser = userService.findByEmail(addUserRequest.getEmail());
        if (registerUser != null) {
            throw new DuplicateException("User Duplicate");
        }
        User user = User.builder()
                .email(addUserRequest.getEmail())
                .username(addUserRequest.getUsername())
                .build();
        user = userService.add(user);
        userRolesService.add(new UserRoles(user.getId(), addUserRequest.getRoleId()));
        return new ResponseEntity<>(user, HttpStatus.OK);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> remove(@PathVariable Long id) throws NotFoundUserException {
        userService.remove(id);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> edit(@PathVariable Long id,
                                  @RequestParam String action,
                                  @RequestBody UpdateUserRequest updateUser) throws NotFoundUserException {
        User registerUser = null;
        if ("UPDATE_PASSWORD".equalsIgnoreCase(action)) {
            registerUser = userService.editPassword(id, updateUser);
        } else if ("RESET_PASSWORD".equalsIgnoreCase(action)) {
            registerUser = userService.resetPassword(id);
        } else if ("EDIT_PROFILE".equalsIgnoreCase(action)) {
            registerUser = userService.editProfile(id, updateUser);
        }
        return new ResponseEntity<>(registerUser, HttpStatus.OK);
    }

}
