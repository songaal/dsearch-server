package com.danawa.dsearch.server.collections;


import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.Map;

public class IndexingJobServiceTest {
    @Test
    public void a(){
        Map<String, Object> convert = new HashMap<>();
        String yamlStr = "scheme: \"http\"\n" +
                "host: \"es2.danawa.io\"\n" +
                "port: 9200\n" +
                "type : \"reindex\"\n" +
                "slices : \"auto\"";

        Map<String, Object> tmp = new Yaml().load(yamlStr);
        if (tmp != null) {
            convert.putAll(tmp);
        }
        System.out.println(convert.toString());
    }
}
