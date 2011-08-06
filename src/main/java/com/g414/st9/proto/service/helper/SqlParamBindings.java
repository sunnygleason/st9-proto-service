package com.g414.st9.proto.service.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.skife.jdbi.v2.SQLStatement;

public class SqlParamBindings {
    private final AtomicInteger next = new AtomicInteger();
    private final Map<String, Pair<Integer, Object>> params = new LinkedHashMap<String, Pair<Integer, Object>>();
    private final boolean positional;

    public SqlParamBindings(boolean positional) {
        this.positional = positional;
    }

    public String bind(String key, Object value) {
        Pair<Integer, Object> pair = params.get(key);

        if (pair == null) {
            pair = new Pair<Integer, Object>(next.getAndIncrement(), value);
        } else {
            pair = new Pair<Integer, Object>(pair.first, value);
        }

        params.put(key, pair);

        return positional ? "?" : ":" + key;
    }

    public String bind(String key) {
        return bind(key, null);
    }

    public void bindToStatement(SQLStatement<?> stmt) {
        for (Map.Entry<String, Pair<Integer, Object>> e : params.entrySet()) {
            Pair<Integer, Object> pair = e.getValue();

            if (positional) {
                stmt.bind(pair.first, pair.second);
            } else {
                stmt.bind(e.getKey(), pair.second);
            }
        }
    }

    public Map<String, Object> asMap() {
        Map<String, Object> toReturn = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, Pair<Integer, Object>> e : params.entrySet()) {
            Pair<Integer, Object> pair = e.getValue();
            toReturn.put(e.getKey(), pair.second);
        }

        return Collections.unmodifiableMap(toReturn);
    }

    public List<Object> asList() {
        ArrayList<Object> toReturn = new ArrayList<Object>(next.get());

        for (Map.Entry<String, Pair<Integer, Object>> e : params.entrySet()) {
            Pair<Integer, Object> pair = e.getValue();
            toReturn.add(pair.first, pair.second);
        }

        return Collections.unmodifiableList(toReturn);
    }

    private static class Pair<K, V> {
        public final K first;
        public final V second;

        private Pair(K first, V second) {
            this.first = first;
            this.second = second;
        }
    }
}
