package com.danawa.fastcatx.server.excpetions;

public class NotFoundUserException extends Exception {
    public NotFoundUserException(String message) {
        super(message);
    }
}
