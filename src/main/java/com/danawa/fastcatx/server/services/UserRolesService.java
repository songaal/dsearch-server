package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.Role;
import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.entity.UserRoles;
import com.danawa.fastcatx.server.repository.UserRepository;
import com.danawa.fastcatx.server.repository.UserRepositorySupport;
import com.danawa.fastcatx.server.repository.UserRolesRepository;
import com.danawa.fastcatx.server.repository.UserRolesRepositorySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class UserRolesService {
    private static Logger logger = LoggerFactory.getLogger(UserRolesService.class);

    private final UserRepositorySupport userRepositorySupport;
    private final UserRepository userRepository;
    private final UserRolesRepository userRolesRepository;
    private final UserRolesRepositorySupport userRolesRepositorySupport;

    public UserRolesService(UserRepositorySupport userRepositorySupport1,
                            UserRepository userRepository1,
                            UserRolesRepository userRolesRepository1,
                            UserRolesRepositorySupport userRolesRepositorySupport) {
        this.userRepositorySupport = userRepositorySupport1;
        this.userRepository = userRepository1;
        this.userRolesRepository = userRolesRepository1;
        this.userRolesRepositorySupport = userRolesRepositorySupport;
    }

    public UserRoles findByUserId(Long id) {
        return userRolesRepositorySupport.findByUserId(id);
    }

    public UserRoles add(UserRoles userRoles) {
        return userRolesRepository.save(userRoles);
    }
}
