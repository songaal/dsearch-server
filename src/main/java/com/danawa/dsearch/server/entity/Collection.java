package com.danawa.dsearch.server.entity;

import java.io.Serializable;
import java.util.Map;

public class Collection implements Serializable {
    private String id; // es index id
    private String name;
    private String baseId;

    private boolean scheduled;
    private String jdbcId;
    private String cron;
    private String sourceName;
    private Launcher launcher;
    private Index indexA;
    private Index indexB;
    private boolean autoRun;
    private Integer replicas;
    private Integer refresh_interval;
    private String ignoreRoleYn;


    public boolean isAutoRun() {
        return autoRun;
    }

    public void setAutoRun(boolean autoRun) {
        this.autoRun = autoRun;
    }

    public static class Index {
        private String index;
        private String health;
        private String status;
        private String uuid;
        private String pri;
        private String rep;
        private String docsCount;
        private String docsDeleted;
        private String storeSize;
        private String priStoreSize;

        private Map<String, Object> aliases;

        public Map<String, Object> getAliases() {
            return aliases;
        }

        public void setAliases(Map<String, Object> aliases) {
            this.aliases = aliases;
        }

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getHealth() {
            return health;
        }

        public void setHealth(String health) {
            this.health = health;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getPri() {
            return pri;
        }

        public void setPri(String pri) {
            this.pri = pri;
        }

        public String getRep() {
            return rep;
        }

        public void setRep(String rep) {
            this.rep = rep;
        }

        public String getDocsCount() {
            return docsCount;
        }

        public void setDocsCount(String docsCount) {
            this.docsCount = docsCount;
        }

        public String getDocsDeleted() {
            return docsDeleted;
        }

        public void setDocsDeleted(String docsDeleted) {
            this.docsDeleted = docsDeleted;
        }

        public String getStoreSize() {
            return storeSize;
        }

        public void setStoreSize(String storeSize) {
            this.storeSize = storeSize;
        }

        public String getPriStoreSize() {
            return priStoreSize;
        }

        public void setPriStoreSize(String priStoreSize) {
            this.priStoreSize = priStoreSize;
        }

    }

    public static class Launcher {
//        private String path;
        private String yaml;
        private String host;
        private int port;

//        public String getPath() {
//            return path;
//        }
//
//        public void setPath(String path) {
//            this.path = path;
//        }

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

        public String getYaml() {
            return yaml;
        }

        public void setYaml(String yaml) {
            this.yaml = yaml;
        }
    }

    public Integer getRefresh_interval() {
        return refresh_interval;
    }

    public Integer getReplicas() {
        return replicas;
    }

    public void setRefresh_interval(Integer refresh_interval) {
        this.refresh_interval = refresh_interval;
    }

    public void setReplicas(Integer replicas) {
        this.replicas = replicas;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
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

    public Index getIndexA() {
        return indexA;
    }

    public void setIndexA(Index indexA) {
        this.indexA = indexA;
    }

    public Index getIndexB() {
        return indexB;
    }

    public void setIndexB(Index indexB) {
        this.indexB = indexB;
    }

    public String getIgnoreRoleYn() {
        return ignoreRoleYn;
    }

    public void setIgnoreRoleYn(String ignoreRoleYn) {
        this.ignoreRoleYn = ignoreRoleYn;
    }

}
