package com.danawa.dsearch.server;

import com.danawa.dsearch.server.services.NoticeHandler;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(properties = {"classpath:/application-default.yml"})
public class TelegramSendTest {

    @Test
    public void sendTest() {


        NoticeHandler.send("메시지 전송!!!");


    }


}
