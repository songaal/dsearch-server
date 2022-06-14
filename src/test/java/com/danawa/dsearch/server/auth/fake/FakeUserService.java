package com.danawa.dsearch.server.auth.fake;

import com.danawa.dsearch.server.auth.repository.UserRepository;
import com.danawa.dsearch.server.auth.repository.UserRepositorySupport;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import com.danawa.dsearch.server.auth.service.UserService;

public class FakeUserService extends UserService {
    public FakeUserService(UserRepositorySupport userRepositorySupport, UserRepository userRepository, UserRolesService userRolesService) {
        super(userRepositorySupport, userRepository, userRolesService);
    }
}
