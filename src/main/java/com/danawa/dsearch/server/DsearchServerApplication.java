package com.danawa.dsearch.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DsearchServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DsearchServerApplication.class, args);
    }
}
