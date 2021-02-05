package com.danawa.dsearch.server;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class JsonTest {
    private static Logger logger = LoggerFactory.getLogger(JsonTest.class);

    @Test
    public void aaa() {
        logger.warn("{}", 1);
    }

//    @Test
//    public void jsonvalidTest() {
//        String json = "";
//
//
//        try {
//            JSONObject jsonSchema = new JSONObject(json);
//            logger.debug("{}", jsonSchema);
//        } catch (JSONException e) {
//            e.printStackTrace();
//        }
////        JSONObject jsonSubject = new JSONObject(new JSONTokener(json));
////        Schema schema = SchemaLoader.load(jsonSchema);
////        schema.validate(jsonSubject);
//
//    }
}
