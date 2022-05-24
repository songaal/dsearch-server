package com.danawa.dsearch.server.notice;

public class Telegram {
    private String enabled;
    private String bot;
    private String chat;

    public String getBot() {
        return bot;
    }

    public void setBot(String bot) {
        this.bot = bot;
    }

    public String getChat() {
        return chat;
    }

    public void setChat(String chat) {
        this.chat = chat;
    }

    public boolean isEnabled() {
        return "true".equalsIgnoreCase(enabled);
    }

    public String getEnabled() {
        return enabled;
    }

    public void setEnabled(String enabled) {
        this.enabled = enabled;
    }

    public boolean validation() {
        return this.bot != null && this.chat != null;
    }

}
