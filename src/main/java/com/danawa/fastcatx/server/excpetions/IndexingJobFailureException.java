package com.danawa.fastcatx.server.excpetions;

public class IndexingJobFailureException extends Exception {

    public IndexingJobFailureException(String m) {
        super(m);
    }
    public IndexingJobFailureException(Exception e) {
        super(e);
    }
}
