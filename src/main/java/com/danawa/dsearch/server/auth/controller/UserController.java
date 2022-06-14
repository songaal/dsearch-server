package com.danawa.dsearch.server.auth.controller;

import com.danawa.dsearch.server.auth.entity.UserProfile;
import com.danawa.dsearch.server.auth.service.UserService;
import com.danawa.dsearch.server.auth.entity.dto.AddUserRequest;
import com.danawa.dsearch.server.auth.entity.dto.UpdateUserRequest;
import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.excpetions.DuplicatedUserException;
import com.danawa.dsearch.server.excpetions.InvalidPasswordException;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import org.apache.commons.lang.NullArgumentException;
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
    public ResponseEntity<?> find(@PathVariable long id) throws NotFoundUserException {
        User user = userService.find(id);
        return new ResponseEntity<>(user, HttpStatus.OK);
    }

    @PostMapping
    public ResponseEntity<?> add(@RequestBody AddUserRequest addUserRequest) throws NullArgumentException, DuplicatedUserException {
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
                                  @RequestBody UpdateUserRequest updateUser) throws NotFoundUserException, InvalidPasswordException {
        User registerUser = null;
        if ("UPDATE_PASSWORD".equalsIgnoreCase(action)) {
            registerUser = userService.editPassword(id, updateUser.getPassword(), updateUser.getUpdatePassword());
        } else if ("RESET_PASSWORD".equalsIgnoreCase(action)) {
            registerUser = userService.resetPassword(id);
        } else if ("EDIT_PROFILE".equalsIgnoreCase(action)) {
            UserProfile profile = new UserProfile(updateUser.getRoleId(), updateUser.getUsername(), updateUser.getEmail());
            registerUser = userService.editProfile(id, profile);
        }
        return new ResponseEntity<>(registerUser, HttpStatus.OK);
    }

}
