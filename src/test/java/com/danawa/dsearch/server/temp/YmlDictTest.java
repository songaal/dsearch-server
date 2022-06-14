package com.danawa.dsearch.server.temp;

import com.danawa.dsearch.server.dictionary.service.DictionaryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.UUID;

@SpringBootTest
public class YmlDictTest {
    @Autowired
    private DictionaryService dictionaryService;


    @Test
    public void infoDictTest() {

        try {
            dictionaryService.getAnalysisPluginSettings(UUID.fromString("94cc2940-dae8-498a-9d99-29ef03a2eea7"));
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
