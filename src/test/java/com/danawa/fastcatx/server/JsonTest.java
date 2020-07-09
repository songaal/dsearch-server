package com.danawa.fastcatx.server;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class JsonTest {
    private static Logger logger = LoggerFactory.getLogger(JsonTest.class);

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
