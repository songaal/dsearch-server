package com.danawa.dsearch.server.utils;

public class TimeUtils {
    public static void sleep(long millis){
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {

        }
    }
}
