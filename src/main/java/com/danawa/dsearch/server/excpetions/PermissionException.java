package com.danawa.dsearch.server.excpetions;

public class PermissionException extends Exception {

    public PermissionException() {
        super("");
    }
    public PermissionException(String message) {
        super(message);
    }
}
