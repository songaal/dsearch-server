package com.danawa.fastcatx.server.excpetions;

public class PermissionException extends Exception {

    public PermissionException() {
        super("");
    }
    public PermissionException(String message) {
        super(message);
    }
}
