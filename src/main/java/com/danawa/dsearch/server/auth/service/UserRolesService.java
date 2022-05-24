package com.danawa.dsearch.server.auth.service;

import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.auth.repository.UserRepository;
import com.danawa.dsearch.server.auth.repository.UserRepositorySupport;
import com.danawa.dsearch.server.auth.repository.UserRolesRepository;
import com.danawa.dsearch.server.auth.repository.UserRolesRepositorySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;


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
    public UserRoles set(UserRoles userRoles) {
        return userRolesRepository.save(userRoles);
    }

    public UserRoles add(UserRoles userRoles) {
        return userRolesRepository.save(userRoles);
    }

    public List<UserRoles> findAll() {
        return userRolesRepository.findAll();
    }

    public UserRoles deleteByUserId(Long id) throws NotFoundUserException {
        UserRoles userRoles = userRolesRepositorySupport.findByUserId(id);
        if (userRoles == null) {
            throw new NotFoundUserException("UserRoles Not Found");
        }
        userRolesRepository.delete(userRoles);
        return userRoles;
    }
}
