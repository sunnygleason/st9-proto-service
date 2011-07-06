package com.g414.st9.proto.service.helper;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.codec.binary.Hex;

public class OpaquePaginationHelper {
    public static String createOpaqueCursor(Long offset) throws Exception {
        Map<String, Object> enc = new LinkedHashMap<String, Object>();
        enc.put("o", offset);
        return Hex.encodeHexString(EncodingHelper.convertToSmileLzf(enc));
    }

    public static Long decodeOpaqueCursor(String token) throws Exception {
        if (token == null || token.length() == 0) {
            return 0L;
        }

        try {
            byte[] tokenValue = Hex.decodeHex(token.toCharArray());
            Map<String, Object> vals = (Map<String, Object>) EncodingHelper
                    .parseSmileLzf(tokenValue);

            return ((Number) vals.get("o")).longValue();
        } catch (Exception e) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("invalid page token: " + token).build());
        }
    }
}
