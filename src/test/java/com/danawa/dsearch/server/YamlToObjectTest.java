package com.danawa.dsearch.server;

import org.yaml.snakeyaml.Yaml;

import java.util.Map;

public class YamlToObjectTest {


    public static void main(String[] args) {
        String doc = null;
        Yaml yaml = new Yaml();
        Map<String, Object> params = yaml.load(doc);

        System.out.println(params);

    }

}
