package com.danawa.dsearch.server.auth.service;

import com.danawa.dsearch.server.auth.entity.Role;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.auth.repository.RoleRepository;
import org.aspectj.weaver.ast.Not;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;

@Service
public class RoleService {
    private static Logger logger = LoggerFactory.getLogger(RoleService.class);

    private final RoleRepository roleRepository;

    public RoleService(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }

    public List<Role> findAll() {
        return roleRepository.findAll();
    }

    public Role find(long id) throws NotFoundUserException {
        Role role = roleRepository.findById(id).orElseThrow(() -> new NotFoundUserException("Not Found User"));
        return role;
    }

    public Role add(Role role) {
        return roleRepository.save(role);
    }

    public Role edit(long id, Role role) throws NotFoundUserException {
        Role registerRole = roleRepository.findById(id).orElseThrow(() -> new NotFoundUserException("Not Found User"));
        // registerRole은 Null이 될 수 없음

        registerRole.setName(role.getName());
        registerRole.setAnalysis(role.isAnalysis());
        registerRole.setIndex(role.isIndex());
        registerRole.setSearch(role.isSearch());
        registerRole.setManage(role.isManage());
        registerRole.setUpdateDate(LocalDateTime.now());
        return roleRepository.save(registerRole);
    }

    public Role remove(long id) throws NotFoundUserException{
        Role registerRole = find(id);
        roleRepository.delete(registerRole);
        return registerRole;
    }

}
