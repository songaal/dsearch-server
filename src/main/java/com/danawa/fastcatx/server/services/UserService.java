package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.UpdateUserRequest;
import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.excpetions.NotFoundException;
import com.danawa.fastcatx.server.repository.UserRepository;
import com.danawa.fastcatx.server.repository.UserRepositorySupport;
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

    public UserService(UserRepositorySupport userRepositorySupport,
                       UserRepository userRepository) {
        this.userRepositorySupport = userRepositorySupport;
        this.userRepository = userRepository;
    }

    public List<User> findAll() {
        return userRepository.findAll();
    }

    public User find(long id) {
        return userRepository.findById(id).get();
    }

    public User add(User user) {
        String tempPassword = UUID.randomUUID().toString().substring(0, 5);
        user.setPassword(encoder.encode(tempPassword));
        user.setId(null);
        User registerUser = userRepository.save(user);
        registerUser.setPassword(tempPassword);
        return registerUser;
    }

    public User set(User user) {
        return userRepository.save(user);
    }

    public User editPassword(Long id, UpdateUserRequest updateUser) throws NotFoundException {
        User registerUser = find(id);
        if (registerUser == null) {
            throw new NotFoundException("Not Found User");
        }
        if(!encoder.matches(updateUser.getPassword(), registerUser.getPassword())) {
            throw new NotFoundException("Invalid password");
        }
        registerUser.setPassword(encoder.encode(updateUser.getUpdatePassword()));
        registerUser = userRepository.save(registerUser);
        registerUser.setPassword(null);
        return registerUser;
    }

    public User remove(Long id) {
        User user = userRepository.findById(id).get();
        userRepository.delete(user);
        return user;
    }

    public User findByEmail(String email) {
        return userRepositorySupport.findByEmail(email);
    }
}
