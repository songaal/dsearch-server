package com.danawa.fastcatx.server.excpetions;

public class ServiceException extends Exception {

    public ServiceException() {
        super("");
    }
    public ServiceException(String message) {
        super(message);
    }
    public ServiceException(Exception e) {
        super(e);
    }
}
