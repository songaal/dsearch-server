package com.danawa.fastcatx.server.services;

import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class DictionaryService {
    private static Logger logger = LoggerFactory.getLogger(DictionaryService.class);

    private RestHighLevelClient client;

    public DictionaryService(@Qualifier("getRestHighLevelClient") RestHighLevelClient restHighLevelClient) {
        this.client = restHighLevelClient;
    }





}
