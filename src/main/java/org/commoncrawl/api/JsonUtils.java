package org.commoncrawl.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonUtils {
	
	public static Map<String, List<String>> jsonToMap(JSONObject json) {
        Map<String, List<String>> retMap = new HashMap<String, List<String>>();

        if(json != JSONObject.NULL) {
            retMap = toMap(json);
        }
        return retMap;
    }

    public static Map<String, List<String>> toMap(JSONObject object) throws JSONException {
        Map<String, List<String>> map = new HashMap<String, List<String>>();

        Iterator<String> keysItr = object.keySet().iterator();
        while(keysItr.hasNext()) {
            String key = keysItr.next();
            JSONArray array = object.getJSONArray(key);
            List<String> list = new ArrayList<String>();
            for(int i = 0; i < array.length(); i++) {
                String value = array.get(i).toString();
                list.add(value);
            }
           
            map.put(key, list);
        }
        return map;
    }

    public static List<String> toList(JSONArray array) {
        List<String> list = new ArrayList<String>();
        for(int i = 0; i < array.length(); i++) {
            String value = array.get(i).toString();
            list.add(value);
        }
        return list;
    }
    /*
    public static Map<String, Object> jsonToMap(JsonObject json) {
        Map<String, Object> retMap = new HashMap<String, Object>();

        if(json != JsonObject.NULL) {
            retMap = toMap(json);
        }
        return retMap;
    }

    public static Map<String, Object> toMap(JsonObject object) throws JsonException {
        Map<String, Object> map = new HashMap<String, Object>();

        Iterator<String> keysItr = object.keySet().iterator();
        while(keysItr.hasNext()) {
            String key = keysItr.next();
            Object value = object.get(key);

            if(value instanceof JsonArray) {
                value = toList((JsonArray) value);
            }

            else if(value instanceof JsonObject) {
                value = toMap((JsonObject) value);
            }
            map.put(key, value);
        }
        return map;
    }

    public static List<Object> toList(JsonArray array) {
        List<Object> list = new ArrayList<Object>();
        for(int i = 0; i < array.size(); i++) {
            Object value = array.get(i);
            if(value instanceof JsonArray) {
                value = toList((JsonArray) value);
            }

            else if(value instanceof JsonObject) {
                value = toMap((JsonObject) value);
            }
            list.add(value);
        }
        return list;
    }
   */
	
}