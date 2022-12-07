package com.danawa.dsearch.server.excpetions;

public class ParameterInvalidException extends Exception {
    public ParameterInvalidException(String message) {
        super(message);
    }
    public ParameterInvalidException(Exception e) {
        super(e);
    }
}
