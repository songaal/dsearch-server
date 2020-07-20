package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.services.IndexingJobService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

@SpringBootTest
public class IndexingJobTest {
    private static Logger logger = LoggerFactory.getLogger(IndexingJobTest.class);
    @Autowired
    private IndexingJobService indexingJobService;

    @Test
    public void indexTest() {
//        try {
////            indexingJobService.indexing(UUID.fromString("6db92f95-a3c3-4f0c-926a-9c9fdbac281e"), "test");
//        } catch (Exception e) {
//            logger.error("", e);
//        }
    }

}
