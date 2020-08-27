package com.danawa.dsearch.server.excpetions;

public class ElasticQueryException extends Exception {

    public ElasticQueryException(String message) {
        super(message);
    }
    public ElasticQueryException(Exception e) {
        super(e);
    }
}
