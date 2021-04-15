package com.danawa.dsearch.server.config;

import com.danawa.dsearch.indexer.IndexJobManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class IndexerConfig {

    @Bean
    public IndexJobManager indexJobManager() {
        return new IndexJobManager();
    }
    public enum ACTION { FULL_INDEX, }

}
