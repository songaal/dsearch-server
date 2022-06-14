package com.danawa.dsearch.server.auth;

import com.danawa.dsearch.server.auth.entity.AuthUser;
import com.danawa.dsearch.server.auth.entity.Role;
import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.auth.fake.FakeAuthService;
import com.danawa.dsearch.server.auth.service.AuthService;
import com.danawa.dsearch.server.auth.service.RoleService;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import com.danawa.dsearch.server.auth.service.UserService;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.utils.JWTUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class AuthServiceTest {

    @Mock
    private UserService userService;
    @Mock
    private RoleService roleService;
    @Mock
    private UserRolesService userRolesService;

    private AuthService authService;

    private String username = "admin";
    private String password = "admin";
    private String email = "admin@example.com";
    private String secret = "dsearch-secret";
    private Long expirationTimeMillis = 7200000L;

    @BeforeEach
    public void setup(){
        this.authService = new FakeAuthService(username, password, email, secret, expirationTimeMillis, userService, roleService, userRolesService );
    }

    @Test
    @DisplayName("로그인 성공")
    public void signIn_success() throws NotFoundUserException {
        // given
        Long userId = 1L;
        Long roleId = 1L;
        String roleName = "roleName";

        PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        String encodedPassword = encoder.encode(password);

        User loginUser = new User(username, password, email);
        loginUser.setId(userId);

        User registerdUser = new User(username, encodedPassword, email);
        registerdUser.setId(userId);

        UserRoles userRoles = UserRoles.builder().userId(userId).roleId(roleId).build();
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();

        given(userService.findByEmail(email)).willReturn(registerdUser);
        given(userRolesService.findByUserId(userId)).willReturn(userRoles);
        given(roleService.find(userId)).willReturn(role);

        // when
        AuthUser authUser = authService.signIn(loginUser);

        // then
        Assertions.assertEquals(roleName, authUser.getRole().getName());
        Assertions.assertEquals(loginUser.getId(), authUser.getUser().getId());
    }

    @Test
    @DisplayName("로그인 실패, 등록되지 않은 유저 로그인 시도")
    public void signIn_fail_when_try_to_login_not_registerd_user() {
        // given
        Long userId = 1L;

        User loginUser = new User(username, password, email);
        loginUser.setId(userId);

        given(userService.findByEmail(email)).willReturn(null);

        // when
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            // then
            AuthUser authUser = authService.signIn(loginUser);
        });
    }

    @Test
    @DisplayName("로그인 실패, 틀린 패스워드로 로그인 시도")
    public void signIn_fail_when_wrong_password() {
        // given
        Long userId = 1L;
        Long roleId = 1L;
        String roleName = "roleName";
        String wrongPassword = "wrong";

        PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        String encodedPassword = encoder.encode(password);

        User loginUser = new User(username, wrongPassword, email);
        loginUser.setId(userId);

        User registerdUser = new User(username, encodedPassword, email);
        registerdUser.setId(userId);

        UserRoles userRoles = UserRoles.builder().userId(userId).roleId(roleId).build();
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();

        given(userService.findByEmail(email)).willReturn(registerdUser);

        // when
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            // then
            AuthUser authUser = authService.signIn(loginUser);
        });

    }

    @Test
    @DisplayName("JWT로 유저 찾기 성공")
    public void findAuthUserByToken_success() throws NotFoundUserException {

        Long userId = 1L;
        Long roleId = 1L;
        String roleName = "roleName";

        UserRoles userRoles = UserRoles.builder().userId(userId).roleId(roleId).build();
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        User user = new User(username, password, email);
        user.setId(userId);

        AuthUser authUser = new AuthUser(user, role);
        JWTUtils jwtUtils = new JWTUtils(secret, expirationTimeMillis);
        String token = jwtUtils.sign(authUser);

        given(userService.find(userId)).willReturn(user);
        given(userRolesService.findByUserId(userId)).willReturn(userRoles);
        given(roleService.find(userId)).willReturn(role);

        // when
        AuthUser newAuthUser = authService.findAuthUserByToken(token);

        // then
        Assertions.assertEquals(userId, newAuthUser.getUser().getId());
        Assertions.assertEquals(roleName, newAuthUser.getRole().getName());
    }

    @Test
    @DisplayName("JWT로 유저 찾기 실패, 토큰이 잘못된 토큰 일 경우")
    public void findAuthUserByToken_fail_when_wrong_token() {
        Long userId = 1L;

        User user = new User(username, password, email);
        user.setId(userId);

        // when
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            //then
            AuthUser newAuthUser = authService.findAuthUserByToken(null);
        });

        // when
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            //then
            AuthUser newAuthUser = authService.findAuthUserByToken("wrong token");
        });
    }
}
