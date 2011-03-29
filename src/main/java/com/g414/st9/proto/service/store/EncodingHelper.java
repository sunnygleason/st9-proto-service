package com.g414.st9.proto.service.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.codec.net.URLCodec;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;

import com.g414.codec.lzf.LZFCodec;

public class EncodingHelper {
    private static final SmileFactory smileFactory = new SmileFactory();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final LZFCodec compressCodec = new LZFCodec();
    private static final URLCodec cacheKeyCodec = new URLCodec();
    private static final String KV_CACHE_PREFIX = "kv:";

    public static String convertToJson(Map<String, Object> value)
            throws Exception {
        return mapper.writeValueAsString(value);
    }

    public static Map<String, Object> parseJsonString(String value) {
        if (value == null || value.length() == 0) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'value'").build());
        }

        try {
            return (Map<String, Object>) mapper.readValue(value,
                    LinkedHashMap.class);
        } catch (Exception e) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("Invalid entity 'value'").build());
        }
    }

    public static byte[] convertToSmileLzf(Object value) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            SmileGenerator smile = smileFactory.createJsonGenerator(out);
            mapper.writeValue(smile, value);

            byte[] smileBytes = out.toByteArray();
            byte[] lzfBytes = compressCodec.encode(smileBytes);

            return lzfBytes;
        } catch (Exception e) {
            throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    public static Object parseSmileLzf(byte[] valueBytesLzf) {
        try {
            byte[] valueBytes = compressCodec.decode(valueBytesLzf);
            ByteArrayInputStream in = new ByteArrayInputStream(valueBytes);
            SmileParser smile = smileFactory.createJsonParser(in);

            return mapper.readValue(smile, LinkedHashMap.class);
        } catch (Exception e) {
            throw new WebApplicationException(e, Status.INTERNAL_SERVER_ERROR);
        }
    }

    public static String toKVCacheKey(String key) throws Exception {
        return cacheKeyCodec.encode(KV_CACHE_PREFIX + key);
    }

    public static String fromKVCacheKey(String cachekey) throws Exception {
        return new String(cacheKeyCodec.decode(cachekey
                .substring(KV_CACHE_PREFIX.length())));
    }
}
