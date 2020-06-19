package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.Role;
import com.danawa.fastcatx.server.repository.RoleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class RoleService {
    private static Logger logger = LoggerFactory.getLogger(RoleService.class);

    private final RoleRepository roleRepository;

    public RoleService(RoleRepository roleRepository) {
        this.roleRepository = roleRepository;
    }

    public Role find(long id) {
        return roleRepository.findById(id).get();
    }

    public Role add(Role role) {
        return roleRepository.save(role);
    }

    public Role edit(long id, Role role) {
        role.setId(id);
        return roleRepository.save(role);
    }

    public Role remove(long id) {
        Role registerRole = find(id);
        roleRepository.delete(registerRole);
        return registerRole;
    }
}
