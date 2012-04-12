package com.g414.st9.proto.service;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;
import com.google.inject.internal.Nullable;

/**
 * Resource for performing fulltext stuff.
 */
@Path("/1.0/f")
public class FulltextResource {
    @Inject
    private KeyValueStorage store;

    private final DateTimeFormatter format = ISODateTimeFormat
            .basicDateTimeNoMillis();

    @POST
    @Path("replay")
    public Response createEntity(@Nullable @QueryParam("from") String from,
            @Nullable @QueryParam("to") String to) throws Exception {
        DateTime toDateTime = (to == null) ? (new DateTime()).plusDays(1)
                : format.parseDateTime(to);

        DateTime fromDateTime = (from == null) ? toDateTime.minusYears(1000)
                : format.parseDateTime(from);

        return this.store.replayUpdates(fromDateTime, toDateTime);
    }
}
