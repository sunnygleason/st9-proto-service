package com.g414.st9.proto.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.g414.st9.proto.service.schema.FulltextDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.sequence.SequenceService;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.internal.Nullable;

/**
 * Resource for performing fulltext stuff.
 */
@Path("/1.0/f")
public class FulltextResource {
    @Inject
    private SequenceService sequences;

    @Inject
    private KeyValueStorage store;

    private final ObjectMapper mapper = new ObjectMapper();

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

    @GET
    @Path("mappings")
    public Response getMappings() throws Exception {
        final Iterator<Map<String, Object>> iter = this.store
                .iterator("$schema");

        return Response.status(Status.OK).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException,
                    WebApplicationException {
                while (iter.hasNext()) {
                    Map<String, Object> value = iter.next();
                    try {
                        if (value == null || value.containsKey("$deleted")) {
                            continue;
                        }

                        Key theKey = Key.valueOf((String) value.remove("id"));
                        value.remove("kind");
                        value.remove("version");

                        String jsonValue = mapper.writeValueAsString(value);
                        SchemaDefinition schema = mapper.readValue(jsonValue,
                                SchemaDefinition.class);

                        String type = sequences.getTypeName(theKey.getId()
                                .intValue());

                        for (FulltextDefinition fulltext : schema
                                .getFulltexts()) {
                            if (fulltext.getParentType() == null) {
                                continue;
                            }

                            Map<String, Object> outValue = new LinkedHashMap<String, Object>();
                            Map<String, Object> nested = new LinkedHashMap<String, Object>();
                            outValue.put(type, nested);
                            nested.put("_parent", fulltext.getParentType());

                            output.write(mapper.writeValueAsString(outValue)
                                    .getBytes());
                            output.write('\n');
                            output.flush();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        mapper.writeValue(output, ImmutableMap
                                .<String, Object> of("error",
                                        "unable to process schema", "data",
                                        value));
                        output.write('\n');
                        output.flush();
                    }
                }
                output.close();
            }
        }).build();
    }
}
