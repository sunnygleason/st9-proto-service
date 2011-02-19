package com.g414.st9.proto.service;

import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.sun.faban.driver.HttpClientFacade;
import com.sun.faban.driver.HttpRequestMethod;

public class KeyValueOperations {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String BASE_URL = "/1.0/e";

    public static void create(String host, int port, HttpClientFacade f,
            String key, Map<String, Object> value) throws Exception {
        String type = (String) value.get("kind");
        String json = mapper.writeValueAsString(value);

        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + type)
                .withHeader("Content-Type", "application/json")
                .withHeader("Accept", "application/json")
                .withRequestMethod(HttpRequestMethod.POST)
                .withPostRequest(json).execute();
    }

    public static void retrieve(String host, int port, HttpClientFacade f,
            String key) throws Exception {
        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + key)
                .withHeader("Accept", "application/json")
                .withRequestMethod(HttpRequestMethod.GET).execute();
    }

    public static void multiRetrieve(String host, int port, HttpClientFacade f,
            List<String> keys) throws Exception {
        StringBuilder queryString = new StringBuilder();
        for (String key : keys) {
            queryString.append("&k=");
            queryString.append(key);
        }

        f.asReadRequest()
                .withUrl(
                        "http://" + host + ":" + port + BASE_URL + "/multi?"
                                + queryString.toString())
                .withHeader("Accept", "application/json")
                .withRequestMethod(HttpRequestMethod.GET).execute();
    }

    public static void update(String host, int port, HttpClientFacade f,
            String key, Map<String, Object> value) throws Exception {
        String json = mapper.writeValueAsString(value);

        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + key)
                .withHeader("Content-Type", "application/json")
                .withHeader("Accept", "application/json")
                .withRequestMethod(HttpRequestMethod.PUT).withPostRequest(json)
                .execute();
    }

    public static void delete(String host, int port, HttpClientFacade f,
            String key) throws Exception {
        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + key)
                .withRequestMethod(HttpRequestMethod.DELETE).execute();
    }
}
