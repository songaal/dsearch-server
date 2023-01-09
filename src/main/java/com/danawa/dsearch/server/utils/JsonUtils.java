package com.danawa.dsearch.server.utils;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class JsonUtils {

    public static Map<String, Object> convertStringToMap(String data){
        return createCustomGson().fromJson(data, new TypeToken<Map<String, Object>>(){}.getType());
    }

    public static String convertMapToString(Map<String, Object> data){
        return createCustomGson().toJson(data);
    }


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

    public static Gson createCustomGson(){
        JsonUtils.CustomGsonAdapter adapter = new JsonUtils.CustomGsonAdapter();
        Gson gson = new GsonBuilder().registerTypeAdapter(Map.class, adapter).create();
        return gson;
    }

    private static class CustomGsonAdapter extends TypeAdapter<Object> {
        private final TypeAdapter<Object> delegate = new Gson().getAdapter(Object.class);

        @Override
        public void write(JsonWriter out, Object value) throws IOException {
            delegate.write(out, value);
        }

        @Override
        public Object read(JsonReader in) throws IOException {
            JsonToken token = in.peek();
            switch (token) {
                case BEGIN_ARRAY:
                    List<Object> list = new ArrayList<Object>();
                    in.beginArray();
                    while (in.hasNext()) {
                        list.add(read(in));
                    }
                    in.endArray();
                    return list;

                case BEGIN_OBJECT:
                    Map<String, Object> map = new LinkedTreeMap<String, Object>();
                    in.beginObject();
                    while (in.hasNext()) {
                        map.put(in.nextName(), read(in));
                    }
                    in.endObject();
                    return map;

                case STRING:
                    return in.nextString();

                case NUMBER:
                    //return in.nextDouble();
                    String n = in.nextString();
                    if (n.indexOf('.') != -1) {
                        return Double.parseDouble(n);
                    }
                    return Long.parseLong(n);
                case BOOLEAN:
                    return in.nextBoolean();
                case NULL:
                    in.nextNull();
                    return null;
                default:
                    throw new IllegalStateException();
            }
        }
    }

}
