package com.danawa.fastcatx.server.controller;

import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.services.AuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

@RestController
@RequestMapping("/auth")
public class AuthController {
    private static Logger logger = LoggerFactory.getLogger(AuthController.class);

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    //    TODO 로그인 (권한부여)
    @PostMapping("/login")
    public ResponseEntity<?> login(HttpSession session,
                                   @RequestBody User user) {



        return new ResponseEntity<>(HttpStatus.OK);
    }

    //    TODO 로그아웃 (권한회수)
    public ResponseEntity<?> logout() {
        return new ResponseEntity<>(HttpStatus.OK);
    }

    //    TODO 인증 (권한 확인)
    public ResponseEntity<?> getAuth() {
        return new ResponseEntity<>(HttpStatus.OK);
    }


}
