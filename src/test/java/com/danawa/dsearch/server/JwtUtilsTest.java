package com.danawa.dsearch.server;

import com.danawa.dsearch.server.entity.AuthUser;
import com.danawa.dsearch.server.entity.Role;
import com.danawa.dsearch.server.entity.User;
import com.danawa.dsearch.server.utils.JWTUtils;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
public class JwtUtilsTest {
    private static Logger logger = LoggerFactory.getLogger(JwtUtilsTest.class);

    private static JWTUtils jwtUtils = new  JWTUtils("test", 60000L);

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {

        }

    }

    @Test
    void signTest() {
        User user = new User("test", "123", "test@server.com");
        Role role = new Role("role", true, true, true, true);

        AuthUser authUser = new AuthUser(user, role);

        String token;

        token = jwtUtils.sign(authUser);
        logger.info("new >> {}", token);

    }


}
