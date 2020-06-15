package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.services.DictionaryService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class DictionaryTest {

    @Autowired
    private DictionaryService dictionaryService;

    @Test
    public void downloadTest() {
        try {

            dictionaryService.getSettings();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
