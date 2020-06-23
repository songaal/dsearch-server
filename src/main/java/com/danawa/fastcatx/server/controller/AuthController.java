package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.AuthUser;
import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.excpetions.NotFoundException;
import com.danawa.fastcatx.server.services.AuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;

@RestController
@RequestMapping("/auth")
public class AuthController {
    private static Logger logger = LoggerFactory.getLogger(AuthController.class);

    private int maxInactiveInterval = 2 * (60 * 60);
    public static final String SESSION_KEY = "AUTH_USER";
    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @GetMapping
    public ResponseEntity<?> getAuth(HttpSession session) throws NotFoundException {
        AuthUser authuser = (AuthUser) session.getAttribute(SESSION_KEY);
        if (authuser == null) {
            throw new NotFoundException("Not Found User");
        }
        return new ResponseEntity<>(authuser, HttpStatus.OK);
    }

    @PostMapping("/sign-in")
    public ResponseEntity<?> signIn(HttpSession session,
                                   @RequestBody User user) throws NotFoundException {
        AuthUser authUser = authService.signIn(session.getId(), user);
        session.setAttribute(SESSION_KEY, authUser);
        session.setMaxInactiveInterval(maxInactiveInterval);
        return new ResponseEntity<>(authUser, HttpStatus.OK);
    }

    @PostMapping("/sign-out")
    public ResponseEntity<?> signOut(HttpSession session) {
        session.setMaxInactiveInterval(0);
        session.invalidate();
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
