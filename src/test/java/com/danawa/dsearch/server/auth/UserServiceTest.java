package com.danawa.dsearch.server.auth;

import com.danawa.dsearch.server.auth.entity.User;
import com.danawa.dsearch.server.auth.entity.UserProfile;
import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.auth.fake.FakeUserService;
import com.danawa.dsearch.server.auth.repository.UserRepository;
import com.danawa.dsearch.server.auth.repository.UserRepositorySupport;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import com.danawa.dsearch.server.auth.service.UserService;
import com.danawa.dsearch.server.excpetions.DuplicatedUserException;
import com.danawa.dsearch.server.excpetions.InvalidPasswordException;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import org.apache.commons.lang.NullArgumentException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
public class UserServiceTest {
    private UserService userService;

    @Mock
    private UserRepositorySupport userRepositorySupport;

    @Mock
    private UserRepository userRepository;

    @Mock
    private UserRolesService userRolesService;

    @BeforeEach
    public void setup() {
        this.userService = new FakeUserService(userRepositorySupport, userRepository, userRolesService);
    }

    @Test
    @DisplayName("User 전체 찾기 성공")
    public void find_all_success(){
        // given
        given(userRepository.findAll()).willReturn(new ArrayList<User>());

        // when
        List<User> list = userService.findAll();

        //then
        Assertions.assertEquals(0, list.size());
    }

    @Test
    @DisplayName("User id로 찾기 성공")
    public void find_by_id_success() throws NotFoundUserException {
        // given
        long id = 1L;
        String email = "admin@example.com";
        User user = User.builder().email(email).build();
        user.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.of(user));

        // when
        User result = userService.find(id);

        //then
        Assertions.assertEquals(id, result.getId());
    }

    @Test
    @DisplayName("User id로 찾기 실패, 해당 id로 검색 했으나 없는 경우")
    public void find_by_id_fail() {
        // given
        long id = 1L;
        String email = "admin@example.com";
        User user = User.builder().email(email).build();
        user.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.empty());

        // then
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            //when
            User result = userService.find(id);
        });
    }

    @Test
    @DisplayName("User 추가 성공")
    public void add_user_success() throws DuplicatedUserException {
        // given
        long id = 1L;
        String email = "admin@example.com";
        String username = "username";
        String password = UUID.randomUUID().toString().substring(0, 5);
        User user = User.builder().username(username).password(password).email(email).build();
        User savedUser = User.builder().username(username).password(password).email(email).build();
        savedUser.setId(id);
        given(userRepositorySupport.findByEmail(email)).willReturn(null);
        given(userRepository.save(user)).willReturn(savedUser);

        // when
        User result = userService.add(user);

        //then
        Assertions.assertEquals(username, result.getUsername());
        Assertions.assertEquals(email, result.getEmail());
        Assertions.assertEquals(1L, result.getId());
    }

    @Test
    @DisplayName("User 추가 실패, 이미 있는 유저")
    public void add_user_fail_when_exist_user() {
        // given
        String email = "admin@example.com";
        String username = "username";
        String password = UUID.randomUUID().toString().substring(0, 5);
        User user = User.builder().username(username).password(password).email(email).build();
        given(userRepositorySupport.findByEmail(email)).willReturn(user);

        // then
        Assertions.assertThrows(DuplicatedUserException.class, ()->{
            // when
            User result = userService.add(user);
        });
    }

    @Test
    @DisplayName("User 추가 실패, User 인스턴스가 null 이거나 필수 변수들이 null")
    public void add_user_fail_when_not_input_email() {
        // given
        User user = User.builder().build();

        // then
        Assertions.assertThrows(NullArgumentException.class, ()->{
            // when
            User result = userService.add(null);
        });

        // then
        Assertions.assertThrows(NullArgumentException.class, ()->{
            // when
            User result = userService.add(user);
        });

        user.setUsername("username");
        // then
        Assertions.assertThrows(NullArgumentException.class, ()->{
            // when
            User result = userService.add(user);
        });

        user.setEmail("admin@example.com");
        // then
        Assertions.assertThrows(NullArgumentException.class, ()->{
            // when
            User result = userService.add(user);
        });
    }

    @Test
    @DisplayName("User 비밀 번호 변경 성공")
    public void edit_user_password_success() throws NotFoundUserException, InvalidPasswordException {
        // given
        long id = 1L;
        String updatePassword = UUID.randomUUID().toString().substring(0, 5);
        String password = UUID.randomUUID().toString().substring(0, 5);
        PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        String encodedPassword = encoder.encode(password);
        User registerUser = User.builder().password(encodedPassword).build();
        registerUser.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.of(registerUser));
        given(userRepository.save(registerUser)).willReturn(registerUser);

        // when
        User result = userService.editPassword(id, password, updatePassword);

        //then
        Assertions.assertEquals(1L, result.getId());
    }

    @Test
    @DisplayName("User 비밀 번호 변경 실패, 등록된 유저가 없을 경우")
    public void edit_user_password_fail_when_not_exist_user() throws NotFoundUserException {
        // given
        long id = 1L;
        String updatePassword = UUID.randomUUID().toString().substring(0, 5);
        String password = UUID.randomUUID().toString().substring(0, 5);
        given(userRepository.findById(id)).willReturn(Optional.empty());
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            // when
            User result = userService.editPassword(id, password, updatePassword);
        });
    }

    @Test
    @DisplayName("User 비밀 번호 변경 실패, 패스워드가 일치하지 않을 경우")
    public void edit_user_password_fail_when_not_match_password() throws NotFoundUserException {
        // given
        long id = 1L;
        String updatePassword = UUID.randomUUID().toString().substring(0, 5);
        String password = UUID.randomUUID().toString().substring(0, 5);
        PasswordEncoder encoder = PasswordEncoderFactories.createDelegatingPasswordEncoder();
        String encodedPassword = encoder.encode(password);
        User registerUser = User.builder().password(encodedPassword).build();
        registerUser.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.of(registerUser));

        Assertions.assertThrows(InvalidPasswordException.class, () -> {
            String wrongPassword = UUID.randomUUID().toString().substring(0, 5);
            // when
            User result = userService.editPassword(id, wrongPassword, updatePassword);
        });
    }

    @Test
    @DisplayName("User 삭제 성공")
    public void remove_user_success() throws NotFoundUserException {
        // given
        long id = 1L;
        User user = User.builder().build();
        user.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.of(user));
        given(userRolesService.deleteByUserId(id)).willReturn(null);
        doNothing().when(userRepository).delete(user);

        // when
        User result = userService.remove(id);

        //then
        Assertions.assertEquals(id, result.getId());
    }

    @Test
    @DisplayName("User 삭제 실패, 등록되지 않은 유저")
    public void remove_user_fail_when_not_exist_user() {
        // given
        long id = 1L;
        User user = User.builder().build();
        user.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.empty());

        //then
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            // when
            User result = userService.remove(id);
        });
    }

    @Test
    @DisplayName("User 패스워드 리셋 성공")
    public void reset_user_password_success() throws NotFoundUserException {
        // given
        long id = 1L;
        String password = UUID.randomUUID().toString().substring(0, 5);
        User user = User.builder().username("admin").password(password).email("admin@example.com").build();
        user.setId(id);
        given(userRepository.findById(id)).willReturn(Optional.of(user));
        given(userRepository.save(user)).willReturn(user);

        // when
        User result = userService.resetPassword(id);

        //then
        Assertions.assertNotEquals(password, result.getPassword());
    }

    @Test
    @DisplayName("User 패스워드 리셋 실패, 등록되지 않은 유저")
    public void reset_user_password_fail_when_not_exist_user() throws NotFoundUserException {
        // given
        long id = 1L;
        given(userRepository.findById(id)).willReturn(Optional.empty());

        // then
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            // when
            User result = userService.resetPassword(id);
        });
    }

    @Test
    @DisplayName("User 프로필 변경 성공")
    public void edit_user_profile_success() throws NotFoundUserException {
        // given
        long id = 1L;
        long roleId = 1L;
        String username = "username";
        String email = "email";
        UserProfile profile = new UserProfile(roleId, username, email);

        User user = User.builder().username(username).email(email).build();
        user.setId(id);
        UserRoles userRoles = new UserRoles(id, roleId);
        given(userRepository.findById(id)).willReturn(Optional.of(user));
        given(userRepository.save(user)).willReturn(user);
        given(userRolesService.findByUserId(id)).willReturn(userRoles);
        given(userRolesService.add(userRoles)).willReturn(userRoles);

        // when
        User result = userService.editProfile(id, profile);

        //then
        Assertions.assertEquals(username, result.getUsername());
        Assertions.assertEquals(email, result.getEmail());
    }

    @Test
    @DisplayName("User 프로필 변경 실패, 유저가 없을 경우")
    public void edit_user_profile_fail_when_not_exist_user(){
        // given
        long id = 1L;
        long roleId = 1L;
        String username = "";
        String email = "";
        UserProfile profile = new UserProfile(roleId, username, email);

        User user = User.builder().username(username).email(email).build();
        user.setId(id);
        UserRoles userRoles = new UserRoles(id, roleId);
        given(userRepository.findById(id)).willReturn(Optional.empty());

        //then
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            // when
            User result = userService.editProfile(id, profile);
        });
    }
}
