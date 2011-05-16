package com.g414.st9.proto.service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;

import com.g414.st9.proto.service.cache.KeyValueCache;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * A ping endpoint that performs deep checks by default.
 */
@Path("/ping")
public class PingResource {
    private static final String PING_CACHE_PREFIX = "ping:";

    @Inject
    @Named("db.prefix")
    private String prefix;

    @Inject
    private IDBI database;

    @Inject
    private KeyValueCache cache;

    private final DateTimeFormatter formatter = ISODateTimeFormat
            .basicDateTimeNoMillis();

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response createEntity(@QueryParam("deep") Boolean deep)
            throws Exception {
        boolean isDeep = deep == null || deep;

        if (isDeep) {
            if (cache.isPersistent()) {
                try {
                    String cacheKey = PING_CACHE_PREFIX
                            + UUID.randomUUID().toString();
                    String nowString = formatter.print(new DateTime())
                            .toString();

                    cache.put(cacheKey, nowString.getBytes());

                    byte[] cacheBytes = cache.get(cacheKey);
                    if (cacheBytes == null) {
                        return Response.status(Status.INTERNAL_SERVER_ERROR)
                                .entity("no cache present").build();
                    }

                    String getString = new String(cacheBytes);
                    if (!getString.equals(nowString)) {
                        return Response.status(Status.INTERNAL_SERVER_ERROR)
                                .entity("cache returned invalid value").build();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return Response.status(Status.INTERNAL_SERVER_ERROR)
                            .entity(e.getMessage()).build();
                }
            }

            try {
                Response response = database
                        .inTransaction(new TransactionCallback<Response>() {
                            @Override
                            public Response inTransaction(Handle handle,
                                    TransactionStatus status) throws Exception {
                                List<Map<String, Object>> result = handle
                                        .createQuery(prefix + "ping").list();

                                if (result == null
                                        || result.size() != 1
                                        || !result.get(0).get("1").toString()
                                                .equals("1")) {
                                    return Response
                                            .status(Status.INTERNAL_SERVER_ERROR)
                                            .entity("database returned invalid result")
                                            .build();
                                }

                                return null;
                            }
                        });

                if (response != null) {
                    return response;
                }
            } catch (Exception e) {
                e.printStackTrace();
                return Response.status(Status.INTERNAL_SERVER_ERROR)
                        .entity(e.getMessage()).build();
            }
        }

        return Response.status(Status.OK).entity("OK").build();
    }
}
