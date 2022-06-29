package com.danawa.dsearch.server.auth;

import com.danawa.dsearch.server.auth.entity.UserRoles;
import com.danawa.dsearch.server.auth.repository.UserRepository;
import com.danawa.dsearch.server.auth.repository.UserRepositorySupport;
import com.danawa.dsearch.server.auth.repository.UserRolesRepository;
import com.danawa.dsearch.server.auth.repository.UserRolesRepositorySupport;
import com.danawa.dsearch.server.auth.service.UserRolesService;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
public class UserRolesServiceTest {

    private UserRolesService userRolesService;

    @Mock
    private UserRepositorySupport userRepositorySupport;
    @Mock
    private UserRepository userRepository;
    @Mock
    private UserRolesRepository userRolesRepository;
    @Mock
    private UserRolesRepositorySupport userRolesRepositorySupport;

    @BeforeEach
    public void setup(){
        this.userRolesService = new UserRolesService(userRepositorySupport, userRepository, userRolesRepository, userRolesRepositorySupport);
    }

    @Test
    @DisplayName("user-role 관계 전부 가져오기")
    public void find_all_success(){
        // given
        given(userRolesRepository.findAll()).willReturn(new ArrayList<UserRoles>());

        // when
        List<UserRoles> list = userRolesService.findAll();

        //then
        Assertions.assertEquals(0, list.size());
    }


    @Test
    @DisplayName("user-role 관계 id로 가져오기")
    public void find_by_id_success(){
        // given
        Long userId = 1L;
        Long roleId = 1L;
        UserRoles userRoles = UserRoles.builder().userId(userId).roleId(roleId).build();
        given(userRolesRepositorySupport.findByUserId(userId)).willReturn(userRoles);

        // when
        UserRoles result = userRolesService.findByUserId(userId);

        //then
        Assertions.assertEquals(userId, result.getUserId());
    }

    @Test
    @DisplayName("user-role 관계 추가하기")
    public void add_user_roles_success(){
        // given
        Long id = 1L;
        Long userId = 2L;
        Long roleId = 3L;

        UserRoles userRoles = UserRoles.builder().userId(userId).roleId(roleId).build();
        userRoles.setId(id);
        given(userRolesRepository.save(userRoles)).willReturn(userRoles);

        // when
        UserRoles result = userRolesService.add(userRoles);

        //then
        Assertions.assertEquals(id, result.getId());
        Assertions.assertEquals(userId, result.getUserId());
        Assertions.assertEquals(roleId, result.getRoleId());
    }

    @Test
    @DisplayName("user-role 관계 삭제")
    public void delete_by_userId_success() throws NotFoundUserException {
        // given
        Long id = 1L;
        Long userId = 2L;
        Long roleId = 3L;

        UserRoles userRoles = UserRoles.builder().userId(userId).roleId(roleId).build();
        userRoles.setId(id);
        given(userRolesRepositorySupport.findByUserId(userId)).willReturn(userRoles);
        doNothing().when(userRolesRepository).delete(userRoles);

        // when
        UserRoles result = userRolesService.deleteByUserId(userId);

        //then
        Assertions.assertEquals(id, result.getId());
        Assertions.assertEquals(userId, result.getUserId());
        Assertions.assertEquals(roleId, result.getRoleId());
    }

    @Test
    @DisplayName("user-role 관계 삭제 실패, id로 조회했지만 유저가 없었을 때")
    public void delete_by_userId_fail_when_not_fount_user()  {
        // given
        Long userId = 1L;
        given(userRolesRepositorySupport.findByUserId(userId)).willReturn(null);

        // when
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            UserRoles result = userRolesService.deleteByUserId(userId);
        });
    }
}
