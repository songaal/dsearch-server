package com.danawa.dsearch.server.services;

import com.danawa.dsearch.server.entity.Telegram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "dsearch.notice")
public class NoticeHandler {
    private final static Logger logger = LoggerFactory.getLogger(NoticeHandler.class);

    private static Telegram telegram;
    private static WebClient telegramWebClient;
    private static Map<String, Object> telegramBody;

    @PostConstruct
    public void setup() {
        if (telegram.validation() && telegram.isEnabled()) {
            telegramWebClient = WebClient.builder().build();
            telegramBody = new HashMap<>();
            telegramBody.put("chat_id", telegram.getChat());
            send("DSearch 서버 시작하였습니다.");
        }

    }

    public static void send(String message) {
        if (telegramWebClient != null) {
            telegramBody.put("text", message);

            telegramWebClient
                    .mutate()
                    .baseUrl("https://api.telegram.org")
                    .build()
                    .post()
                    .uri(String.format("/bot%s/sendMessage", telegram.getBot()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(telegramBody)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

        }
    }

    public Telegram getTelegram() {
        return telegram;
    }

    public void setTelegram(Telegram telegram) {
        this.telegram = telegram;
    }
}
