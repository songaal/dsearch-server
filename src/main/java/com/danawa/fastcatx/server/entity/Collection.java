package com.danawa.fastcatx.server.entity;

import java.io.Serializable;

public class Collection implements Serializable {
    private String id;
    private String name;
    private String baseId;
    private String indexA;
    private String indexB;
    private boolean scheduled;
    private String jdbcId;
    private String cron;
    private Launcher launcher;


    public static class Launcher {
        private String param;
        private String host;
        private int port;

        public String getParam() {
            return param;
        }

        public void setParam(String param) {
            this.param = param;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBaseId() {
        return baseId;
    }

    public void setBaseId(String baseId) {
        this.baseId = baseId;
    }

    public String getIndexA() {
        return indexA;
    }

    public void setIndexA(String indexA) {
        this.indexA = indexA;
    }

    public String getIndexB() {
        return indexB;
    }

    public void setIndexB(String indexB) {
        this.indexB = indexB;
    }

    public boolean isScheduled() {
        return scheduled;
    }

    public void setScheduled(boolean scheduled) {
        this.scheduled = scheduled;
    }

    public String getJdbcId() {
        return jdbcId;
    }

    public void setJdbcId(String jdbcId) {
        this.jdbcId = jdbcId;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public Launcher getLauncher() {
        return launcher;
    }

    public void setLauncher(Launcher launcher) {
        this.launcher = launcher;
    }
}
