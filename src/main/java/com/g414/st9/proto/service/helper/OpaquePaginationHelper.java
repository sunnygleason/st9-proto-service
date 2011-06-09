package com.g414.st9.proto.service.helper;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;


public class OpaquePaginationHelper {
    public static final Long DEFAULT_PAGE_SIZE = 25L;

    public static String createOpaqueCursor(Long offset) throws Exception {
        Map<String, Object> enc = new LinkedHashMap<String, Object>();
        enc.put("o", offset);
        return Hex.encodeHexString(EncodingHelper.convertToSmileLzf(enc));
    }

    public static Long decodeOpaqueCursor(String token) throws Exception {
        if (token == null) {
            return 0L;
        }

        byte[] tokenValue = Hex.decodeHex(token.toCharArray());
        Map<String, Object> vals = (Map<String, Object>) EncodingHelper
                .parseSmileLzf(tokenValue);

        return ((Number) vals.get("o")).longValue();
    }
}
