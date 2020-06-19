package com.danawa.fastcatx.server.entity;

import java.io.Serializable;
import java.util.List;

public class AuthUser implements Serializable {

    private User user;
    private List<Role> roles;

}
