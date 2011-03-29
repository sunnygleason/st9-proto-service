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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.index.OpaquePaginationHelper;
import com.g414.st9.proto.service.query.QueryLexer;
import com.g414.st9.proto.service.query.QueryParser;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.store.EncodingHelper;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does secondary index search
 * operations for KV documents using a "real" db index.
 */
@Path("/1.0/i")
public class SecondaryIndexResource {
    @Inject
    private IDBI database;

    @Inject
    private KeyValueStorage storage;

    @Inject
    private JDBISecondaryIndex index;

    private ObjectMapper mapper = new ObjectMapper();

    @GET
    @Path("{type}.{index}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@PathParam("type") String type,
            @PathParam("index") String indexName,
            @QueryParam("q") String query, @QueryParam("s") String token,
            @QueryParam("n") Long num) throws Exception {
        return doSearch(type, indexName, query, token, num);
    }

    public void clear() {
        this.database.withHandle(new HandleCallback<Void>() {
            @Override
            public Void withHandle(Handle handle) throws Exception {
                index.clear(handle, storage.iterator("$schema"));

                return null;
            }
        });
    }

    /**
     * Perform a search against the specified object type, using the specified
     * index name and query string.
     * 
     * @param type
     * @param index
     * @param query
     * @return
     * @throws Exception
     */
    private Response doSearch(String type, String indexName, String query,
            String token, Long pageSize) throws Exception {
        List<QueryTerm> queryTerms = null;
        try {
            queryTerms = parseQuery(query);
        } catch (Exception e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid query: " + query).build();
        }

        if (pageSize == null || pageSize > 100 || pageSize < 1) {
            pageSize = OpaquePaginationHelper.DEFAULT_PAGE_SIZE;
        }

        Integer typeId = storage.getTypeId(type);

        Response schemaResponse = storage.retrieve("$schema:" + typeId);

        List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();

        if (schemaResponse.getStatus() == 200) {
            try {
                SchemaDefinition definition = mapper.readValue(schemaResponse
                        .getEntity().toString(), SchemaDefinition.class);

                List<Map<String, Object>> allIds = index.doIndexQuery(database,
                        type, indexName, queryTerms, token, pageSize,
                        definition);

                resultIds.addAll(allIds);
            } catch (ValidationException e) {
                return Response.status(Status.BAD_REQUEST)
                        .entity(e.getMessage()).build();
            } catch (Exception other) {
                // do not apply schema
                other.printStackTrace();

                throw other;
            }
        } else {
            return Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + indexName).build();
        }

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> hits = new ArrayList<Map<String, Object>>();

        Map<String, Object> lastId = null;
        if (resultIds.size() > pageSize) {
            lastId = resultIds.remove(pageSize.intValue());
        }

        result.put("kind", type);
        result.put("index", indexName);
        result.put("query", query);
        result.put("results", hits);

        for (Map<String, Object> rec : resultIds) {
            Long id = ((Number) rec.get("_id")).longValue();
            LinkedHashMap<String, Object> hit = new LinkedHashMap<String, Object>();
            hit.put("id", Key.valueOf(type + ":" + id).getEncryptedIdentifier());
            hits.add(hit);
        }

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

    /**
     * Parses the query string using our trusty ANTLR-generated parser.
     * 
     * @param queryString
     * @return a list of QueryTerm instances
     * @throws Exception
     *             if the query is not parseable
     */
    private List<QueryTerm> parseQuery(String queryString) throws Exception {
        QueryLexer lex = new QueryLexer(new ANTLRStringStream(queryString));
        CommonTokenStream tokens = new CommonTokenStream(lex);
        QueryParser parser = new QueryParser(tokens);

        List<QueryTerm> query = new ArrayList<QueryTerm>();
        parser.term_list(query);

        return query;
    }
}
