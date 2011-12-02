package com.g414.st9.proto.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
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
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.sequence.SequenceServiceDatabaseImpl;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;

/**
 * An endpoint for retrieving entities by their unique values.
 */
@Path("/1.0/u")
public class UniqueIndexResource {
    public static final Long DEFAULT_PAGE_SIZE = 100L;

    @Inject
    private IDBI database;

    @Inject
    private KeyValueStorage storage;

    @Inject
    protected SequenceServiceDatabaseImpl sequences;

    @Inject
    private JDBISecondaryIndex index;

    private ObjectMapper mapper = new ObjectMapper();

    @GET
    @Path("{type}.{index}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response retrieveEntity(@PathParam("type") String type,
            @PathParam("index") String indexName,
            @QueryParam("q") String query,
            @QueryParam("includeQuarantine") Boolean includeQuarantine)
            throws Exception {
        return doSearch(type, indexName, query, includeQuarantine);
    }

    /**
     * Perform a search against the specified object type, using the specified
     * index name and query string.
     * 
     * @param type
     * @param counts
     * @param query
     * @return
     * @throws Exception
     */
    private Response doSearch(String type, String indexName, String query,
            Boolean includeQuarantine) throws Exception {
        Integer typeId = null;
        try {
            typeId = sequences.getTypeId(type, false);
        } catch (WebApplicationException e) {
            return e.getResponse();
        }

        List<QueryTerm> queryTerms = null;
        try {
            queryTerms = parseQuery(query);
        } catch (Exception e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid query: " + query).build();
        }

        Long pageSize = 2L;

        Response schemaResponse = storage.retrieve("$schema:" + typeId, false);

        List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();

        if (schemaResponse.getStatus() != 200) {
            return Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + indexName).build();
        }

        try {
            SchemaDefinition definition = mapper.readValue(schemaResponse
                    .getEntity().toString(), SchemaDefinition.class);

            IndexDefinition indexDef = definition.getIndexMap().get(indexName);
            if (indexDef == null) {
                return Response
                        .status(Status.BAD_REQUEST)
                        .entity("schema or index not found " + type + "."
                                + indexName).build();
            }

            if (!indexDef.isUnique()) {
                return Response.status(Status.BAD_REQUEST)
                        .entity("index '" + indexName + "' is not unique")
                        .build();
            }

            List<Map<String, Object>> allIds = index.doIndexQuery(database,
                    type, indexName, queryTerms, null, pageSize, false,
                    definition);

            resultIds.addAll(allIds);
        } catch (ValidationException e) {
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage())
                    .build();
        } catch (WebApplicationException e) {
            return e.getResponse();
        } catch (Exception other) {
            // do not apply schema
            other.printStackTrace();

            throw other;
        }

        if (resultIds == null || resultIds.size() == 0) {
            return Response.status(Status.NOT_FOUND).entity("").build();
        }

        if (resultIds.size() > 1) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("non-unique results!").build();
        }

        Long id = ((Number) resultIds.get(0).get("_id")).longValue();
        String key = Key.valueOf(type + ":" + id).getEncryptedIdentifier();

        return storage.retrieve(key, false);
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
