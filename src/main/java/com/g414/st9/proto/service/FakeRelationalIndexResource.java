package com.g414.st9.proto.service;

import java.util.ArrayList;
import java.util.Iterator;
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

import com.g414.st9.proto.service.query.QueryEvaluator;
import com.g414.st9.proto.service.query.QueryLexer;
import com.g414.st9.proto.service.query.QueryParser;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.store.EncodingHelper;
import com.g414.st9.proto.service.store.KeyValueStorage;
import com.google.inject.Inject;

/**
 * A silly and simple jersey resource that does secondary index search
 * operations for KV documents.
 */
@Path("/1.0/i")
public class FakeRelationalIndexResource {
    @Inject
    private KeyValueStorage store;

    @Inject
    private QueryEvaluator eval;

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
    private Response doSearch(String type, String index, String query)
            throws Exception {
        List<QueryTerm> queryTerms = null;
        try {
            queryTerms = parseQuery(query);
        } catch (Exception e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity("Invalid query: " + query).build();
        }

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> hits = new ArrayList<Map<String, Object>>();

        result.put("kind", type);
        result.put("index", index);
        result.put("query", query);
        result.put("results", hits);

        Iterator<Map<String, Object>> iter = store.iterator(type);
        while (iter.hasNext()) {
            Map<String, Object> instance = iter.next();
            if (eval.matches(instance, queryTerms)) {
                LinkedHashMap<String, Object> hit = new LinkedHashMap<String, Object>();
                hit.put("id", instance.get("id"));

                hits.add(hit);
            }
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
