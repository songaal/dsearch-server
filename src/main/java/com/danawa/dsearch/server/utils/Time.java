package com.danawa.dsearch.server.utils;

public class Time {
    public static void sleep(long millis){
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {

        }
    }
}
