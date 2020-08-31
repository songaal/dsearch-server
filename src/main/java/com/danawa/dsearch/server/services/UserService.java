package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.entity.UpdateUserRequest;
import com.danawa.dsearch.server.entity.User;
import com.danawa.dsearch.server.entity.UserRoles;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.repository.UserRepository;
import com.danawa.dsearch.server.repository.UserRepositorySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;


@Service
public class UserService {
    private static Logger logger = LoggerFactory.getLogger(UserService.class);

    PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
    private final UserRepositorySupport userRepositorySupport;
    private final UserRepository userRepository;
    private final UserRolesService userRolesService;

    public UserService(UserRepositorySupport userRepositorySupport,
                       UserRepository userRepository, UserRolesService userRolesService) {
        this.userRepositorySupport = userRepositorySupport;
        this.userRepository = userRepository;
        this.userRolesService = userRolesService;
    }

    public List<User> findAll() {
        return userRepository.findAll();
    }

    public User find(long id) {
        return userRepository.findById(id).get();
    }

    public User add(User user) {
        String uuid = UUID.randomUUID().toString().substring(0, 5);
        user.setId(null);
        user.setPassword(encoder.encode(uuid));
        userRepository.save(user);

        User registerUser = new User(user.getUsername(), uuid, user.getEmail());
        registerUser.setId(user.getId());
        return registerUser;
    }

    public User set(User user) {
        return userRepository.save(user);
    }

    public User editPassword(Long id, UpdateUserRequest updateUser) throws NotFoundUserException {
        User registerUser = find(id);
        if (registerUser == null) {
            throw new NotFoundUserException("Not Found User");
        }
        if(!encoder.matches(updateUser.getPassword(), registerUser.getPassword())) {
            throw new NotFoundUserException("Invalid password");
        }
        registerUser.setPassword(encoder.encode(updateUser.getUpdatePassword()));
        registerUser = userRepository.save(registerUser);
        registerUser.setPassword(null);
        return registerUser;
    }

    public User remove(Long id) throws NotFoundUserException {
        User user = userRepository.findById(id).get();
        if (user == null) {
            throw new NotFoundUserException("Not Found User");
        }
        userRolesService.deleteByUserId(id);
        userRepository.delete(user);
        return user;
    }

    public User findByEmail(String email) {
        return userRepositorySupport.findByEmail(email);
    }

    public User resetPassword(Long id) {
        String uuid = UUID.randomUUID().toString().substring(0, 5);
        User registerUser = userRepository.findById(id).get();
        registerUser.setPassword(encoder.encode(uuid));
        userRepository.save(registerUser);
        return User.builder()
                .email(registerUser.getEmail())
                .username(registerUser.getUsername())
                .password(uuid)
                .build();
    }

    public User editProfile(Long id, UpdateUserRequest updateUser) throws NotFoundUserException {
        User registerUser = userRepository.findById(id).get();
        if (registerUser == null) {
            throw new NotFoundUserException("Not Found User");
        }
        registerUser.setEmail(updateUser.getEmail());
        registerUser.setUsername(updateUser.getUsername());
        userRepository.save(registerUser);
        UserRoles userRoles = userRolesService.findByUserId(registerUser.getId());
        userRoles.setRoleId(updateUser.getRoleId());
        userRolesService.set(userRoles);
        return User.builder()
                .username(registerUser.getUsername())
                .email(registerUser.getEmail())
                .build();
    }
}
