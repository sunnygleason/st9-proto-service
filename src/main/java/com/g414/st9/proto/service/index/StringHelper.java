package com.g414.st9.proto.service.index;

import java.util.Collection;

public class StringHelper {
    public static String join(String delim, String... values) {
        if (values == null || values.length == 0) {
            return "";
        }

        StringBuilder s = new StringBuilder();

        for (String v : values) {
            s.append(delim);
            s.append(v);
        }

        return s.toString().substring(delim.length());
    }

    public static String join(String delim, Collection<String> values) {
        if (values == null || values.size() == 0) {
            return "";
        }

        StringBuilder s = new StringBuilder();

        for (String v : values) {
            s.append(delim);
            s.append(v);
        }

        return s.toString().substring(delim.length());
    }
}
