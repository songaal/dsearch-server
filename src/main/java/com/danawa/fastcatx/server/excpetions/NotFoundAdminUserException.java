package com.danawa.fastcatx.server.excpetions;

public class NotFoundAdminUserException extends Exception {
    public NotFoundAdminUserException(String message) {
        super(message);
    }
}
