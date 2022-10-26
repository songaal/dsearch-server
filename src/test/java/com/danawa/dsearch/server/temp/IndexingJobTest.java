package com.danawa.dsearch.server.temp;

import com.danawa.dsearch.server.collections.service.indexing.IndexingJobService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IndexingJobTest {
    private static Logger logger = LoggerFactory.getLogger(IndexingJobTest.class);
    @Autowired
    private IndexingJobService indexingJobService;

    @Test
    public void indexTest() {
    }

}
