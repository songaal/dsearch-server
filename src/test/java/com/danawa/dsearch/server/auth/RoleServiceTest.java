package com.danawa.dsearch.server.auth;

import com.danawa.dsearch.server.auth.entity.Role;
import com.danawa.dsearch.server.auth.fake.FakeRoleService;
import com.danawa.dsearch.server.auth.repository.RoleRepository;
import com.danawa.dsearch.server.auth.service.RoleService;
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
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
public class RoleServiceTest {
    private RoleService roleService;

    @Mock
    private RoleRepository roleRepository;

    @BeforeEach
    public void setup() {
        this.roleService = new FakeRoleService(roleRepository);
    }

    @Test
    @DisplayName("Role 전체 찾기 성공")
    public void findAll_success(){
        //given
        List<Role> list = new ArrayList<>();
        given(roleRepository.findAll()).willReturn(list);

        //when
        List<Role> result = roleService.findAll();

        //then
        Assertions.assertEquals(result.size(), list.size());
    }

    @Test
    @DisplayName("Role 찾기 id 성공")
    public void find_success() throws NotFoundUserException {
        //given
        long id = 1L;
        String roleName = "roleName";
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        role.setId(id);
        given(roleRepository.findById(id)).willReturn(Optional.of(role));

        //when
        Role result = roleService.find(id);

        //then
        Assertions.assertEquals(result.getId(), id);
    }

    @Test
    @DisplayName("Role id로 찾기 실패, 해당하는 role이 없을 경우")
    public void find_fail_when_no_role(){
        //given
        long id = 1L;
        given(roleRepository.findById(id)).willReturn(Optional.empty());

        //when
        Assertions.assertThrows(NoSuchElementException.class, () -> {
            Role result = roleService.find(id);
        });
    }

    @Test
    @DisplayName("Role 저장하기")
    public void add_role_success(){
        //given
        long id = 1L;
        String roleName = "roleName";
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        Role returnedRole = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        returnedRole.setId(id);
        given(roleRepository.save(role)).willReturn(returnedRole);

        Role result = roleService.add(role);

        Assertions.assertEquals(returnedRole.getName(), result.getName());
    }

    @Test
    @DisplayName("Role 수정하기 성공")
    public void edit_role_success() throws NotFoundUserException {
        //given
        long id = 1L;
        String roleName = "roleName";
        String roleName2 = "roleName2";
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        role.setId(id);
        given(roleRepository.findById(id)).willReturn(Optional.of(role));
        role.setName(roleName2);
        given(roleRepository.save(role)).willReturn(role);

        //when
        Role result = roleService.edit(id, role);

        //then
        Assertions.assertEquals(result.getName(), roleName2);
    }

    @Test
    @DisplayName("Role 수정하기 실패, 저장된 role이 없을 경우")
    public void edit_role_fail_when_no_role()  {
        //given
        long id = 1L;
        String roleName = "roleName";
        String roleName2 = "roleName2";
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        role.setId(id);
        given(roleRepository.findById(id)).willReturn(Optional.ofNullable(null));

        // then
        Assertions.assertThrows(NotFoundUserException.class, () -> {
            //when
            Role result = roleService.edit(id, role);
        });
    }

    @Test
    @DisplayName("Role 삭제하기 성공")
    public void remove_role_success() throws NotFoundUserException{
        //given
        long id = 1L;
        String roleName = "roleName";
        Role role = Role.builder().name(roleName).index(true).manage(true).search(true).analysis(true).build();
        role.setId(id);

        given(roleRepository.findById(id)).willReturn(Optional.of(role));
        doNothing().when(roleRepository).delete(role);


        Role result = roleService.remove(id);
        Assertions.assertEquals(role.getName(), result.getName());
    }

    @Test
    @DisplayName("Role 삭제하기 실패, id가 다른 경우")
    public void remove_role_fail_when_id_is_wrong(){
        //given
        long id = 1L;
        String roleName = "roleName";

        given(roleRepository.findById(id)).willReturn(Optional.empty());

        Assertions.assertThrows(NotFoundUserException.class, () -> {
            Role result = roleService.remove(id);
        });
    }
}
