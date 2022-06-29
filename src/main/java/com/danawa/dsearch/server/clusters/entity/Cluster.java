package com.danawa.dsearch.server.clusters.entity;

import org.hibernate.annotations.CreationTimestamp;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "CLUSTER")
public class Cluster {
    @Id
    @GeneratedValue
    private UUID id;
    @Column(nullable = false)
    private String name;
    @Column(nullable = false)
    private String scheme;
    @Column(nullable = false)
    private String host;
    @Column(nullable = false)
    private int port;
    @Column
    private String kibana;
    @Column
    private String theme;
    @CreationTimestamp
    @Column(name = "create_date", nullable = false, updatable = false)
    private LocalDateTime createDate;
    @CreationTimestamp
    @Column(name = "update_date", nullable = false)
    private LocalDateTime updateDate;
    @Column
    private String username;
    @Column
    private String password;

    @Column
    private String dictionaryRemoteClusterId;

    @Column
    private String autocompleteUrl;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getScheme() {
        return scheme;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
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

    public String getKibana() {
        return kibana;
    }

    public void setKibana(String kibana) {
        this.kibana = kibana;
    }

    public String getTheme() {
        return theme;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }

    public LocalDateTime getCreateDate() {
        return createDate;
    }

    public void setCreateDate(LocalDateTime createDate) {
        this.createDate = createDate;
    }

    public LocalDateTime getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(LocalDateTime updateDate) {
        this.updateDate = updateDate;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAutocompleteUrl() {
        return this.autocompleteUrl;
    }
    public void setAutocompleteUrl(String autocompleteUrl) {
        this.autocompleteUrl = autocompleteUrl;
    }

    public String getDictionaryRemoteClusterId() {
        return dictionaryRemoteClusterId;
    }

    public void setDictionaryRemoteClusterId(String dictionaryRemoteClusterId) {
        this.dictionaryRemoteClusterId = dictionaryRemoteClusterId;
    }
}
