package com.danawa.fastcatx.server.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class AuthService {
    private static Logger logger = LoggerFactory.getLogger(AuthService.class);

    private final UserService userService;

    public AuthService(UserService userService) {
        this.userService = userService;
    }


}
