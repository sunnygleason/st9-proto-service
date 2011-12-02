package com.g414.st9.proto.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.map.ObjectMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.g414.st9.proto.service.count.JDBICountService;
import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.helper.OpaquePaginationHelper;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.Attribute;
import com.g414.st9.proto.service.schema.CounterAttribute;
import com.g414.st9.proto.service.schema.CounterDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.sequence.SequenceServiceDatabaseImpl;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does counter reporting for KV
 * documents using a "real" db index.
 */
@Path("/1.0/c")
public class CounterResource {
    public static final Long DEFAULT_PAGE_SIZE = 1000L;

    @Inject
    private IDBI database;

    @Inject
    private KeyValueStorage storage;

    @Inject
    protected SequenceServiceDatabaseImpl sequences;

    @Inject
    private JDBICountService counts;

    private ObjectMapper mapper = new ObjectMapper();

    @GET
    @Path("/{type}.{counter}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveCountersNoBindings(@PathParam("type") String type,
            @PathParam("counter") String counterName, @Context UriInfo uriPath,
            @QueryParam("s") String token, @QueryParam("n") Long num)
            throws Exception {
        return doSearch(type, counterName, uriPath, token, num);
    }

    @GET
    @Path("/{type}.{counter}/{extra:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveCounters(@PathParam("type") String type,
            @PathParam("counter") String counterName, @Context UriInfo uriPath,
            @QueryParam("s") String token, @QueryParam("n") Long num)
            throws Exception {
        return doSearch(type, counterName, uriPath, token, num);
    }

    public void clear(final boolean preserveSchema) {
        this.database.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                counts.clear(handle, storage.iterator("$schema", null),
                        preserveSchema);

                return null;
            }
        });
    }

    /**
     * Retrieve counter entries for the specified object type, using the
     * specified counter name and query string.
     * 
     * @param type
     * @param counts
     * @param query
     * @return
     * @throws Exception
     */
    private Response doSearch(String type, String counterName, UriInfo theUri,
            String token, Long pageSize) throws Exception {
        if (pageSize == null || pageSize > 1000 || pageSize < 1) {
            pageSize = DEFAULT_PAGE_SIZE;
        }

        Integer typeId = sequences.getTypeId(type, false);

        Response schemaResponse = storage.retrieve("$schema:" + typeId, false);

        List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();

        Map<String, Object> queryMap = new LinkedHashMap<String, Object>();

        if (schemaResponse.getStatus() == 200) {
            try {
                SchemaDefinition definition = mapper.readValue(schemaResponse
                        .getEntity().toString(), SchemaDefinition.class);

                List<QueryTerm> queryTerms = parseQuery(definition, type,
                        counterName, theUri);

                for (QueryTerm term : queryTerms) {
                    queryMap.put(term.getField(), term.getValue().getValue());
                }

                List<Map<String, Object>> allIds = counts.doCounterQuery(
                        database, type, counterName, queryTerms, token,
                        pageSize, definition);

                resultIds.addAll(allIds);
            } catch (ValidationException e) {
                return Response.status(Status.BAD_REQUEST)
                        .entity(e.getMessage()).build();
            } catch (WebApplicationException e) {
                return e.getResponse();
            } catch (Exception other) {
                // do not apply schema
                other.printStackTrace();

                throw other;
            }
        } else {
            return Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + counterName).build();
        }

        Map<String, Object> result = new LinkedHashMap<String, Object>();

        Map<String, Object> lastId = null;
        if (resultIds.size() > pageSize) {
            lastId = resultIds.remove(pageSize.intValue());
        }

        result.put("kind", type);
        result.put("counter", counterName);
        result.put("query", queryMap);
        result.put("results", resultIds);

        Long offset = OpaquePaginationHelper.decodeOpaqueCursor(token);
        String theNext = (lastId != null) ? OpaquePaginationHelper
                .createOpaqueCursor(offset + pageSize) : null;
        String thePrev = (offset >= pageSize) ? OpaquePaginationHelper
                .createOpaqueCursor(offset - pageSize) : null;

        result.put("pageSize", pageSize);
        result.put("next", theNext);
        result.put("prev", thePrev);

        String valueJson = EncodingHelper.convertToJson(result);

        return Response.status(Status.OK).entity(valueJson).build();
    }

    private List<QueryTerm> parseQuery(SchemaDefinition definition,
            String type, String counterName, UriInfo theUri)
            throws ValidationException {
        CounterDefinition counterDefinition = definition.getCounterMap().get(
                counterName);

        if (counterDefinition == null) {
            throw new ValidationException("schema or index not found " + type
                    + "." + counterName);
        }

        List<PathSegment> segments = theUri.getPathSegments();
        if (segments.size() < 3) {
            throw new ValidationException("invalid request uri: "
                    + theUri.toString());
        }

        List<PathSegment> params = segments.subList(3, segments.size());
        List<CounterAttribute> attrs = counterDefinition.getCounterAttributes();

        if (params.size() > attrs.size()) {
            throw new ValidationException(
                    "path contains too many counter parameters ("
                            + params.size() + " instead of " + attrs.size()
                            + " or less)");
        }

        List<QueryTerm> terms = new ArrayList<QueryTerm>();

        for (int i = 0; i < params.size(); i++) {
            PathSegment param = params.get(i);
            CounterAttribute counterAttr = attrs.get(i);
            Attribute attr = definition.getAttributesMap().get(
                    counterAttr.getName());

            if (attr == null) {
                throw new ValidationException("unknown counter attribute: "
                        + counterAttr.getName());
            }

            if (param.getPath().isEmpty()) {
                throw new ValidationException(
                        "counter parameter may not be empty");
            }

            terms.add(new QueryTerm(QueryOperator.EQ, attr.getName(),
                    getQueryValue(attr, param.getPath())));
        }

        return terms;
    }

    private QueryValue getQueryValue(Attribute attr, String literal) {
        ValueType type = ValueType.STRING;
        switch (attr.getType()) {
        case BOOLEAN:
            type = ValueType.BOOLEAN;
            break;
        case UTC_DATE_SECS:
        case UTF8_SMALLSTRING:
        case ENUM:
        case REFERENCE:
            type = ValueType.STRING;
            literal = "\"" + literal + "\"";
            break;
        case I8:
        case I16:
        case I32:
        case I64:
        case U8:
        case U16:
        case U32:
        case U64:
            type = ValueType.INTEGER;
            break;
        default:
            throw new ValidationException("unsupported attribute type: "
                    + attr.getType().name());
        }

        return new QueryValue(type, literal);
    }
}
