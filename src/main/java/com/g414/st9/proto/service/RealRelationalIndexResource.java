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
import org.skife.jdbi.v2.IDBI;

import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.query.QueryLexer;
import com.g414.st9.proto.service.query.QueryParser;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.store.EncodingHelper;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does secondary index search
 * operations for KV documents using a "real" db index.
 */
@Path("/1.0/i2")
public class RealRelationalIndexResource {
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
            @PathParam("index") String index, @QueryParam("q") String query)
            throws Exception {
        return doSearch(type, index, query);
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
    private Response doSearch(String type, String indexName, String query)
            throws Exception {
        List<QueryTerm> queryTerms = null;
        try {
            queryTerms = parseQuery(query);
        } catch (Exception e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid query: " + query).build();
        }

        Integer typeId = storage.getTypeId(type);

        Response schemaResponse = storage.retrieve("$schema:" + typeId);

        List<Long> resultIds = new ArrayList<Long>();

        if (schemaResponse.getStatus() == 200) {
            try {
                SchemaDefinition definition = mapper.readValue(schemaResponse
                        .getEntity().toString(), SchemaDefinition.class);

                resultIds.addAll(index.doIndexQuery(database, type, indexName,
                        queryTerms, definition));
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

        result.put("kind", type);
        result.put("index", indexName);
        result.put("query", query);
        result.put("results", hits);

        for (Long id : resultIds) {
            LinkedHashMap<String, Object> hit = new LinkedHashMap<String, Object>();
            hit.put("id", type + ":" + id);
            hits.add(hit);
        }

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
