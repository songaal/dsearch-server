package com.danawa.dsearch.server.auth.service;

import com.danawa.dsearch.server.auth.entity.UserProfile;
import com.danawa.dsearch.server.auth.entity.dto.UpdateUserRequest;
import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.excpetions.DuplicatedUserException;
import com.danawa.dsearch.server.excpetions.InvalidPasswordException;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.auth.repository.UserRepository;
import com.danawa.dsearch.server.auth.repository.UserRepositorySupport;
import org.apache.commons.lang.NullArgumentException;
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
                       UserRepository userRepository,
                       UserRolesService userRolesService) {
        this.userRepositorySupport = userRepositorySupport;
        this.userRepository = userRepository;
        this.userRolesService = userRolesService;
    }

    public List<User> findAll() {
        return userRepository.findAll();
    }

    public User find(long id) throws NotFoundUserException {
        return userRepository.findById(id).orElseThrow(() -> new NotFoundUserException("Not Found User") );
    }

    public User add(User user) throws DuplicatedUserException, NullArgumentException {
        if(user == null || user.getUsername() == null || user.getEmail() == null || user.getPassword() == null){
            throw new NullArgumentException("User is Null or User instance variables(name, email, password) is null");
        }

        User registerUser = findByEmail(user.getEmail());
        if (registerUser != null) {
            throw new DuplicatedUserException("User Duplicate");
        }

        String uuid = UUID.randomUUID().toString().substring(0, 5);
        user.setId(null);
        user.setPassword(encoder.encode(uuid));
        User savedUser = userRepository.save(user);

        registerUser = new User(savedUser.getUsername(), uuid, savedUser.getEmail());
        registerUser.setId(savedUser.getId());
        return registerUser;
    }

    public User addDefaultSystemManager(User systemManager) {
        return userRepository.save(systemManager);
    }

    public User editPassword(Long id, String password, String updatePassword) throws NotFoundUserException, InvalidPasswordException {
        User registerUser = find(id);
        if (registerUser == null) {
            throw new NotFoundUserException("Not Found User");
        }

        if(!encoder.matches(password, registerUser.getPassword())) {
            throw new InvalidPasswordException("Invalid password");
        }

        registerUser.setPassword(encoder.encode(updatePassword));
        registerUser = userRepository.save(registerUser);
        registerUser.setPassword(null);
        return registerUser;
    }

    public User remove(Long id) throws NotFoundUserException {
        User user = userRepository.findById(id).orElseThrow(() -> new NotFoundUserException("Not Found User"));
        userRolesService.deleteByUserId(id);
        userRepository.delete(user);
        return user;
    }

    public User findByEmail(String email) {
        return userRepositorySupport.findByEmail(email);
    }

    public User resetPassword(Long id) throws NotFoundUserException {
        String uuid = UUID.randomUUID().toString().substring(0, 5);
        User registerUser = userRepository.findById(id).orElseThrow(() -> new NotFoundUserException("Not Found User"));
        registerUser.setPassword(encoder.encode(uuid));
        userRepository.save(registerUser);
        return User.builder()
                .email(registerUser.getEmail())
                .username(registerUser.getUsername())
                .password(uuid)
                .build();
    }

    public User editProfile(Long id, UserProfile profile) throws NotFoundUserException {
        User registerUser = userRepository.findById(id).orElseThrow(() -> new NotFoundUserException("Not Found User"));
        registerUser.setEmail(profile.getEmail());
        registerUser.setUsername(profile.getUsername());
        userRepository.save(registerUser);
        UserRoles userRoles = userRolesService.findByUserId(registerUser.getId());
        userRoles.setRoleId(profile.getRoleId());
        userRolesService.add(userRoles);
        return User.builder()
                .username(registerUser.getUsername())
                .email(registerUser.getEmail())
                .build();
    }
}
