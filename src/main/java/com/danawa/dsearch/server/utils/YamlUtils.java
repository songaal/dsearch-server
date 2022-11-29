package com.danawa.dsearch.server.utils;

import com.danawa.dsearch.server.excpetions.ConvertYamlException;
import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.Map;

public class YamlUtils {
    public static Map<String, Object> convertYamlToMap(String yamlStr) throws ConvertYamlException{
        Map<String, Object> convert = new HashMap<>();
        try {
            Map<String, Object> tmp = new Yaml().load(yamlStr);
            if (tmp != null) {
                convert.putAll(tmp);
            }
        } catch (ClassCastException | NullPointerException e) {
            throw new ConvertYamlException(e.getMessage());
        }
        return convert;
    }
}
