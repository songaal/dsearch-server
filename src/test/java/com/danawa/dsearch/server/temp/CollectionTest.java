package com.danawa.dsearch.server.temp;

import com.danawa.dsearch.server.config.ElasticsearchFactory;
import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.collections.service.IndexingJobService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class CollectionTest {
    private static Logger logger = LoggerFactory.getLogger(CollectionTest.class);

    @Autowired
    private IndexingJobService indexingJobService;
    @Autowired
    private ElasticsearchFactory elasticsearchFactory;
    @Autowired
    private CollectionService collectionService;

    @Test
    public void indexingTest(){

    }


    @Test
    public void propagateTest() {
    }


}
