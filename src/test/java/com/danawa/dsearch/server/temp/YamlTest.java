package com.danawa.dsearch.server.temp;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.Map;

public class YamlTest {

    public static void main(String[] args) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);

        Map<String, Object> in = new HashMap<>();
        in.put("aaa", "123123");

        Map<String, Object> inApp = new HashMap<>();
        in.put("app", inApp);
        String out = new Yaml(options).dump(in);

        System.out.println(out);

    }

}
