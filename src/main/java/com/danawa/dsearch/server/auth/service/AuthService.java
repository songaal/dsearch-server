package com.danawa.dsearch.server.auth.service;

import com.auth0.jwt.interfaces.Claim;
import com.danawa.dsearch.server.auth.entity.AuthUser;
import com.danawa.dsearch.server.auth.entity.Role;
import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.utils.JWTUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class AuthService {
    private static Logger logger = LoggerFactory.getLogger(AuthService.class);

    private PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
    private JWTUtils jwtUtils;

    private final String SYSTEM_MANAGER_ROLE = "SYSTEM_MANAGER";
    private String username;
    private String password;
    private String email;

    private final UserService userService;
    private final RoleService roleService;
    private final UserRolesService userRolesService;

    public AuthService(@Value("${dsearch.admin.username}") String username,
                       @Value("${dsearch.admin.password}") String password,
                       @Value("${dsearch.admin.email}") String email,
                       @Value("${dsearch.auth.secret}") String secret,
                       @Value("${dsearch.auth.expiration-time-millis}") Long expirationTimeMillis,
                       UserService userService,
                       RoleService roleService,
                       UserRolesService userRolesService) {
        this.username = username;
        this.password = password;
        this.email = email;
        this.userService = userService;
        this.roleService = roleService;
        this.userRolesService = userRolesService;
        this.jwtUtils = new JWTUtils(secret, expirationTimeMillis);
    }

    @PostConstruct
    public void setup() {
//        SYSTEM MANAGER
        User systemManager = userService.findByEmail(email);
        if (systemManager == null) {
            User systemManagerUser = new User(username, encoder.encode(password), email);
            systemManagerUser = userService.addDefaultSystemManager(systemManagerUser);
            Role systemManagerRole = roleService.add(Role.builder()
                    .name(SYSTEM_MANAGER_ROLE)
                    .analysis(true)
                    .index(true)
                    .search(true)
                    .manage(true)
                    .build()
            );
            userRolesService.add(new UserRoles(systemManagerUser.getId(), systemManagerRole.getId()));
            logger.debug("CREATED USER INFO: {}, ROLE INFO: {}", systemManagerUser, systemManagerRole);
        }
    }

    public AuthUser signIn(User loginUser) throws NotFoundUserException {
        User registerUser = userService.findByEmail(loginUser.getEmail());
        if (registerUser == null) {
            throw new NotFoundUserException("Not Found User");
        }
        if (!encoder.matches(loginUser.getPassword(), registerUser.getPassword())) {
            throw new NotFoundUserException("Not Found User");
        }
        UserRoles userRoles = userRolesService.findByUserId(registerUser.getId());
        Role role = roleService.find(userRoles.getRoleId());
        AuthUser authUser = new AuthUser(registerUser, role);
        authUser.getUser().setPassword(null);
        authUser.setToken(jwtUtils.sign(authUser));
        return authUser;
    }

    public AuthUser findAuthUserByToken(String token) throws NotFoundUserException {
        Claim userIdClaim = jwtUtils.getClaims(token, JWTUtils.USER_ID);
        if (userIdClaim == null) {
            throw new NotFoundUserException("Not Found User");
        }
        User registerUser = userService.find(userIdClaim.asLong());
        UserRoles userRoles = userRolesService.findByUserId(registerUser.getId());
        Role role = roleService.find(userRoles.getRoleId());
        AuthUser authUser = new AuthUser(registerUser, role);
        authUser.getUser().setPassword(null);
        return authUser;
    }

}
