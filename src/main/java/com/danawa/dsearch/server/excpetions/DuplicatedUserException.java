package com.danawa.dsearch.server.excpetions;

public class DuplicatedUserException extends Exception {
    public DuplicatedUserException(String message) {
        super(message);
    }
}
