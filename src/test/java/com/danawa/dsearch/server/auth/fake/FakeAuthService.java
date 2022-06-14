package com.danawa.dsearch.server.auth.fake;

import com.danawa.dsearch.server.auth.entity.AuthUser;
import com.danawa.dsearch.server.auth.entity.Role;
import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.auth.service.AuthService;
import com.danawa.dsearch.server.auth.service.RoleService;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import com.danawa.dsearch.server.auth.service.UserService;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;

public class FakeAuthService extends AuthService {
    public FakeAuthService(String username, String password, String email, String secret, Long expirationTimeMillis, UserService userService, RoleService roleService, UserRolesService userRolesService) {
        super(username, password, email, secret, expirationTimeMillis, userService, roleService, userRolesService);
    }

    /* do nothing */
    public void setup() {}

}
