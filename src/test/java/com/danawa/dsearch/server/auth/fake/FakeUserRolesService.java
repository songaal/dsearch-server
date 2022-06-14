package com.danawa.dsearch.server.auth.fake;

import com.danawa.dsearch.server.auth.repository.UserRepository;
import com.danawa.dsearch.server.auth.repository.UserRepositorySupport;
import com.danawa.dsearch.server.auth.repository.UserRolesRepository;
import com.danawa.dsearch.server.auth.repository.UserRolesRepositorySupport;
import com.danawa.dsearch.server.auth.service.UserRolesService;

public class FakeUserRolesService extends UserRolesService {
    public FakeUserRolesService(UserRepositorySupport userRepositorySupport1, UserRepository userRepository1, UserRolesRepository userRolesRepository1, UserRolesRepositorySupport userRolesRepositorySupport) {
        super(userRepositorySupport1, userRepository1, userRolesRepository1, userRolesRepositorySupport);
    }
}
