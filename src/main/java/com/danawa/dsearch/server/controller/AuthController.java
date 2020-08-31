package com.danawa.dsearch.server.controller;

import com.danawa.dsearch.server.entity.AuthUser;
import com.danawa.dsearch.server.entity.User;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.services.AuthService;
import com.danawa.dsearch.server.services.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpSession;
import java.util.UUID;

@RestController
@RequestMapping("/auth")
public class AuthController {
    private static Logger logger = LoggerFactory.getLogger(AuthController.class);

    public final static int maxInactiveInterval = 2 * (60 * 60);
    public static final String SESSION_KEY = "AUTH_USER";

    private final AuthService authService;
    private final ClusterService clusterService;

    public AuthController(AuthService authService, ClusterService clusterService) {
        this.authService = authService;
        this.clusterService = clusterService;
    }

    @GetMapping
    public ResponseEntity<?> getAuth(@RequestHeader(required = false, value = "cluster-id") String clusterId,
                                     HttpSession session) throws NotFoundUserException {
        AuthUser authuser = (AuthUser) session.getAttribute(SESSION_KEY);
        if (authuser == null) {
            throw new NotFoundUserException("Not Found User");
        }
        if (clusterId != null && !"".equalsIgnoreCase(clusterId)) {
            authuser.setCluster(clusterService.find(UUID.fromString(clusterId)));
        }
        return new ResponseEntity<>(authuser, HttpStatus.OK);
    }

    @PostMapping("/sign-in")
    public ResponseEntity<?> signIn(HttpSession session,
                                   @RequestBody User user) throws NotFoundUserException {
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
