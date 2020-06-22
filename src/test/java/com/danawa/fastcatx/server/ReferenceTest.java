package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.entity.DocumentPagination;
import com.danawa.fastcatx.server.entity.ReferenceResult;
import com.danawa.fastcatx.server.entity.ReferenceTemp;
import com.danawa.fastcatx.server.services.ReferenceService;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class ReferenceTest {
    private static Logger logger = LoggerFactory.getLogger(ReferenceTest.class);
    private RestHighLevelClient client;
    private ReferenceService referenceService;
    @BeforeEach
    public void ReferenceTest (@Qualifier("getRestHighLevelClient") RestHighLevelClient client,
                               @Autowired ReferenceService referenceService) {
        this.client = client;
        this.referenceService = referenceService;
    }

    @Test
    public void findAllTest() {

        try {
//            List<ReferenceTemp> entityList = referenceService.findAllByUsername();
            List<ReferenceResult> result = referenceService.searchResponseAll("노트북");
            logger.debug("{}", result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
