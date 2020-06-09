package com.danawa.fastcatx.server.utils;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class JsonUtils {


    public boolean validate(String json) {
        boolean isJsonValid;
        try {
            new JSONObject(json);
            isJsonValid = true;
        } catch (JSONException e) {
            isJsonValid = false;
        }
        return isJsonValid;
    }

}
