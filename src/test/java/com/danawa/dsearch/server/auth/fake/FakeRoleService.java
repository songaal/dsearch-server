package com.danawa.dsearch.server.auth.fake;

import com.danawa.dsearch.server.auth.repository.RoleRepository;
import com.danawa.dsearch.server.auth.service.RoleService;

public class FakeRoleService extends RoleService {
    public FakeRoleService(RoleRepository roleRepository) {
        super(roleRepository);
    }
}
