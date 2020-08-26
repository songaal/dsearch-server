package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.entity.Role;
import com.danawa.dsearch.server.excpetions.NotFoundUserException;
import com.danawa.dsearch.server.repository.RoleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

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

    public Role find(long id) {
        return roleRepository.findById(id).get();
    }

    public Role add(Role role) {
        return roleRepository.save(role);
    }

    public Role edit(long id, Role role) throws NotFoundUserException {
        Role registerRole = roleRepository.findById(id).get();
        if (registerRole == null) {
            throw new NotFoundUserException("Not Found Role");
        }
        registerRole.setName(role.getName());
        registerRole.setAnalysis(role.isAnalysis());
        registerRole.setIndex(role.isIndex());
        registerRole.setSearch(role.isSearch());
        registerRole.setManage(role.isManage());
        registerRole.setUpdateDate(LocalDateTime.now());
        return roleRepository.save(registerRole);
    }

    public Role remove(long id) {
        Role registerRole = find(id);
        roleRepository.delete(registerRole);
        return registerRole;
    }

}
