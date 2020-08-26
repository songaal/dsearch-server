package com.danawa.dsearch.server.excpetions;

public class NotFoundUserException extends Exception {
    public NotFoundUserException(String message) {
        super(message);
    }
}
