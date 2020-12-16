package com.danawa.dsearch.server.entity;

public class Telegram {
    private boolean enabled;
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
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean validation() {
        return this.bot != null && this.chat != null;
    }

}
