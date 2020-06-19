package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.User;
import com.danawa.fastcatx.server.excpetions.NotFoundAdminUserException;
import com.danawa.fastcatx.server.repository.UserRepository;
import com.danawa.fastcatx.server.repository.UserRepositorySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;


@Service
public class UserService {
    private static Logger logger = LoggerFactory.getLogger(UserService.class);

    private PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();

    private User systemManager;

    private final UserRepositorySupport userRepositorySupport;
    private final UserRepository userRepository;

    public UserService(@Value("${fastcatx.admin.username}") String username,
                       @Value("${fastcatx.admin.password}") String password,
                       @Value("${fastcatx.admin.email}") String email,
                       UserRepositorySupport userRepositorySupport,
                       UserRepository userRepository) {
        systemManager = new User(username, password, email);
        this.userRepositorySupport = userRepositorySupport;
        this.userRepository = userRepository;
    }

    @PostConstruct
    public void setup() {
        List<User> userList = userRepositorySupport.findAll(systemManager.getUsername());

        boolean findAdmin = false;
        for (int i = 0; i < userList.size(); i++) {
            User user = userList.get(i);
            if (user.getPassword() != null && systemManager.getPassword() != null) {
                findAdmin = encoder.matches(systemManager.getPassword(), user.getPassword());
            }
        }
        if (!findAdmin) {
            systemManager.setPassword(encoder.encode(systemManager.getPassword()));
            systemManager = userRepository.save(systemManager);
            systemManager.setPassword(null);
            logger.debug("UPDATE SYSTEM MANAGER INFO: {}", systemManager);
        }
    }

    public List<User> findAll() {
        return userRepository.findAll();
    }

    public User find(long id) {
        return userRepository.findById(id).get();
    }

    public User add(User user) {
        user.setId(null);
        return userRepository.save(user);
    }

    public User edit(Long id, User user) {
        user.setId(id);
        return userRepository.save(user);
    }

    public User remove(Long id) {
        User user = userRepository.findById(id).get();
        userRepository.delete(user);
        return user;
    }
}
