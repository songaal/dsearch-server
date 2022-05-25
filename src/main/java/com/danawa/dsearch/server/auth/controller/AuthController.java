package com.danawa.dsearch.server.auth.controller;

import com.danawa.dsearch.server.auth.entity.AuthUser;
import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.auth.service.AuthService;
import com.danawa.dsearch.server.clusters.service.ClusterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/auth")
public class AuthController {
    private static Logger logger = LoggerFactory.getLogger(AuthController.class);

    private final AuthService authService;
    private final ClusterService clusterService;

    public AuthController(AuthService authService, ClusterService clusterService) {
        this.authService = authService;
        this.clusterService = clusterService;
    }

    @GetMapping
    public ResponseEntity<?> getAuth(@RequestHeader(required = false, value = "cluster-id") String clusterId,
                                     @RequestHeader(value = "x-bearer-token") String token) throws NotFoundUserException {
        AuthUser authUser = authService.findAuthUserByToken(token);
        if (authUser == null) {
            throw new NotFoundUserException("Not Found User");
        }
        if (clusterId != null && !"".equalsIgnoreCase(clusterId)) {
            authUser.setCluster(clusterService.find(UUID.fromString(clusterId)));
        }
        return new ResponseEntity<>(authUser, HttpStatus.OK);
    }

    @PostMapping("/sign-in")
    public ResponseEntity<?> signIn(@RequestBody User user) throws NotFoundUserException {
        AuthUser authUser = authService.signIn(user);
        return new ResponseEntity<>(authUser, HttpStatus.OK);
    }

    @PostMapping("/sign-out")
    public ResponseEntity<?> signOut() {
        MultiValueMap<String, String> headers = new HttpHeaders();
        headers.set("x-bearer-token", null);
        return new ResponseEntity<>(headers, HttpStatus.OK);
    }

}
