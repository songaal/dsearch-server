package com.danawa.fastcatx.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.springframework.test.util.AssertionErrors.assertEquals;

public class IndexingTest {

    @Test
    public void launchIndexerTest() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder("C:\\Program Files\\Amazon Corretto\\jdk1.8.0_252\\bin\\java.exe",
                "-jar", "C:\\Users\\admin\\Downloads\\indexer-0.1.0.jar");

        processBuilder.inheritIO();
        Process process = processBuilder.start();
        Thread.sleep(5000);
        process.destroy();
//        boolean r = process.waitFor(5000, TimeUnit.MILLISECONDS);
//        System.out.println(">>" + r);
//        int exitCode = process.waitFor();
        System.out.println("2");
//        assertEquals("No errors should be detected", 0, exitCode);
    }
}
