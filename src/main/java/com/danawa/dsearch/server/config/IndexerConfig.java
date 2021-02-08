package com.danawa.dsearch.server.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IndexerConfig {

    @Bean
    public com.danawa.fastcatx.indexer.IndexJobManager indexerJobManager() {
        return new com.danawa.fastcatx.indexer.IndexJobManager();
    }

    public enum ACTION { FULL_INDEX, DYNAMIC_INDEX }

}
