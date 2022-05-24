package com.danawa.dsearch.server;

import com.danawa.dsearch.server.collections.service.CollectionService;
import com.danawa.dsearch.server.collections.service.IndexingJobService;
import com.danawa.dsearch.server.indices.IndicesService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class IndexJobServiceTest {
    private static Logger logger = LoggerFactory.getLogger(IndexJobServiceTest.class);

    @Autowired
    private IndexingJobService indexingJobService;
    @Autowired
    private CollectionService collectionService;
    @Autowired
    private IndicesService indicesService;


    @Test
    public void startTest(){

//        try {
//            UUID testUUID = UUID.fromString("1821fb61-d85b-4223-b980-c5560e6aca34");
//            String collectionId = "fUT3aXMBPWfEq1VaJEMW";
//
////            indexingJobService.indexing(testUUID, collectionId);
//        } catch (Exception e) {
//            logger.error("", e);
//        }
    }


}
