package com.danawa.dsearch.server.utils;

import org.json.JSONException;
import org.json.JSONObject;
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
