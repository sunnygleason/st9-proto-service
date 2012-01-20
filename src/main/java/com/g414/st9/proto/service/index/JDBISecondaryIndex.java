package com.g414.st9.proto.service.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.helper.JDBIHelper;
import com.g414.st9.proto.service.helper.SqlParamBindings;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.schema.AttributeType;
import com.g414.st9.proto.service.schema.IndexAttribute;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.google.inject.Inject;

public class JDBISecondaryIndex {
    private final SecondaryIndexTableHelper tableHelper;
    private final KeyValueCache cache;

    @Inject
    public JDBISecondaryIndex(SecondaryIndexTableHelper tableHelper,
            KeyValueCache cache) {
        this.tableHelper = tableHelper;
        this.cache = cache;
    }

    public void createTable(IDBI database, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        JDBIHelper.createTable(database, tableHelper.getTableDrop(type,
                indexName), tableHelper.getTableDefinition(type, indexName,
                schemaDefinition));
    }

    public void createIndex(IDBI database, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                try {
                    handle.createStatement(
                            tableHelper.getPrefix() + "drop_index")
                            .define("table_name",
                                    tableHelper.getTableName(type, indexName))
                            .define("index_name",
                                    tableHelper.getIndexName(type, indexName))
                            .execute();
                } catch (UnableToExecuteStatementException ok) {
                    // expected case in mysql - this is just best-effort anyway
                }

                handle.createStatement(
                        tableHelper.getIndexDefinition(type, indexName,
                                schemaDefinition)).execute();

                return null;
            }
        });
    }

    public void dropTableAndIndex(Handle handle, final String type,
            final String indexName) {
        handle.createStatement(tableHelper.getTableDrop(type, indexName))
                .execute();

        try {
            handle.createStatement(tableHelper.getPrefix() + "drop_index")
                    .define("table_name",
                            tableHelper.getTableName(type, indexName))
                    .define("index_name",
                            tableHelper.getIndexName(type, indexName))
                    .execute();
        } catch (UnableToExecuteStatementException ok) {
            // expected case in mysql - this is just best-effort anyway
        }
    }

    public void dropTable(IDBI database, final String type,
            final String indexName) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                dropTableAndIndex(handle, type, indexName);

                return null;
            }
        });
    }

    public void insertEntity(Handle handle, final Long id,
            final Map<String, Object> value, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        SqlParamBindings bindings = new SqlParamBindings(true);

        Update insert = handle.createStatement(tableHelper.getInsertStatement(
                type, indexName, schemaDefinition, bindings));

        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);
        bindings.bind("id", id, AttributeType.U64);

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            String attrName = attr.getName();
            if ("id".equals(attrName)) {
                continue;
            } else {
                Object v = value.get(attrName) != null ? value.get(attrName)
                        .toString() : null;

                bindings.bind(attrName,
                        tableHelper.transformAttributeValue(v, attr),
                        schemaDefinition.getAttributesMap().get(attrName)
                                .getType());
            }
        }

        bindings.bindToStatement(insert);

        try {
            insert.execute();
        } catch (UnableToExecuteStatementException e) {
            if (tableHelper.isConstraintViolation(e)) {
                throw new WebApplicationException(Response
                        .status(Status.CONFLICT)
                        .entity("unique index constraint violation").build());
            } else {
                throw e;
            }
        }

        if (indexDefinition.isUnique()) {
            SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                    schemaDefinition);
            String uniqKey = tableHelper.computeIndexKey(type, indexName,
                    indexDefinition, value, transformer);

            try {
                cache.put(uniqKey, id.toString().getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void updateEntity(Handle handle, final Long id,
            final Map<String, Object> value, final Map<String, Object> prev,
            final String type, final String indexName,
            final SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);
        SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                schemaDefinition);

        String origKey = tableHelper.computeIndexKey(type, indexName,
                indexDefinition, prev, transformer);
        String newKey = tableHelper.computeIndexKey(type, indexName,
                indexDefinition, value, transformer);

        if (origKey.equals(newKey)) {
            return;
        }

        SqlParamBindings bindings = new SqlParamBindings(true);

        Update update = handle.createStatement(tableHelper.getUpdateStatement(
                type, indexName, schemaDefinition, bindings));

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            String attrName = attr.getName();
            if ("id".equals(attrName)) {
                continue;
            } else {
                Object v = value.get(attrName) != null ? value.get(attrName)
                        .toString() : null;

                bindings.bind(attrName,
                        tableHelper.transformAttributeValue(v, attr),
                        schemaDefinition.getAttributesMap().get(attrName)
                                .getType());
            }
        }

        bindings.bind("id", id, AttributeType.U64);

        bindings.bindToStatement(update);

        try {
            update.execute();
        } catch (UnableToExecuteStatementException e) {
            if (tableHelper.isConstraintViolation(e)) {
                throw new WebApplicationException(Response
                        .status(Status.CONFLICT)
                        .entity("unique index constraint violation").build());
            } else {
                throw e;
            }
        }

        if (indexDefinition.isUnique()) {
            String oldUniqKey = tableHelper.computeIndexKey(type, indexName,
                    indexDefinition, prev, transformer);
            try {
                cache.delete(oldUniqKey);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            String newUniqKey = tableHelper.computeIndexKey(type, indexName,
                    indexDefinition, value, transformer);
            try {
                cache.put(newUniqKey, id.toString().getBytes());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void deleteEntity(Handle handle, final Long id, final String type,
            final Map<String, Object> value, final String indexName,
            final SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        SqlParamBindings bindings = new SqlParamBindings(true);

        Update delete = handle.createStatement(tableHelper.getDeleteStatement(
                type, indexName, bindings));

        bindings.bind("id", id, AttributeType.U64);
        bindings.bindToStatement(delete);

        delete.execute();

        possiblyPurgeUniqueIndexCache(type, indexName, value, schemaDefinition,
                indexDefinition);
    }

    public void setEntityQuarantine(Handle handle, final Long id,
            final String type, final String indexName, boolean isQuarantined,
            final Map<String, Object> original,
            final SchemaDefinition schemaDefinition) {
        SqlParamBindings bindings = new SqlParamBindings(true);

        Update quarantine = handle.createStatement(tableHelper
                .getQuarantineStatement(type, indexName, bindings,
                        isQuarantined));

        bindings.bind("id", id, AttributeType.U64);
        bindings.bindToStatement(quarantine);

        quarantine.execute();

        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        possiblyPurgeUniqueIndexCache(type, indexName, original,
                schemaDefinition, indexDefinition);
    }

    private void possiblyPurgeUniqueIndexCache(final String type,
            final String indexName, final Map<String, Object> original,
            final SchemaDefinition schemaDefinition,
            IndexDefinition indexDefinition) {
        if (indexDefinition.isUnique()) {
            SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                    schemaDefinition);
            String uniqKey = tableHelper.computeIndexKey(type, indexName,
                    indexDefinition, original, transformer);

            try {
                cache.delete(uniqKey);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void clear(Handle handle, Iterator<Map<String, Object>> schemas,
            boolean preserveSchema) throws Exception {
        while (schemas.hasNext()) {
            Map<String, Object> schema = schemas.next();
            if (schema == null) {
                continue;
            }

            String type = (String) schema.get("$type");

            if (schema != null && schema.get("indexes") != null) {
                for (Map<String, Object> index : (List<Map<String, Object>>) schema
                        .get("indexes")) {
                    String indexName = (String) index.get("name");

                    if (preserveSchema) {
                        this.truncateTable(handle, type, indexName);
                    } else {
                        this.dropTableAndIndex(handle, type, indexName);
                    }
                }
            }
        }
    }

    public List<Map<String, Object>> doIndexQuery(IDBI database, String type,
            String indexName, List<QueryTerm> queryTerms, String token,
            Long pageSize, boolean includeQuarantine,
            SchemaDefinition schemaDefinition) throws Exception {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);
        if (indexDefinition == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + indexName).build());
        }

        final SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                schemaDefinition);

        Map<String, List<QueryTerm>> termMap = tableHelper.sortTerms(
                indexDefinition, queryTerms);

        if (!includeQuarantine && indexDefinition.isUnique()) {
            return tableHelper.doUniqueIndexQuery(database, cache, type,
                    indexName, termMap, token, indexDefinition,
                    schemaDefinition, transformer);
        }

        final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
        final SqlParamBindings bindings = new SqlParamBindings(true);

        final String querySql = tableHelper.getIndexQuery(type, indexName,
                termMap, token, pageSize, includeQuarantine, indexDefinition,
                schemaDefinition, transformer, bindings);

        Response response = database
                .inTransaction(new TransactionCallback<Response>() {
                    @Override
                    public Response inTransaction(Handle handle,
                            TransactionStatus status) throws Exception {
                        try {
                            Query<Map<String, Object>> query = handle
                                    .createQuery(querySql);

                            bindings.bindToStatement(query);

                            for (Map<String, Object> r : query.list()) {
                                resultIds.add(r);
                            }

                            return null;
                        } catch (WebApplicationException e) {
                            return e.getResponse();
                        }
                    }
                });

        if (response != null) {
            throw new WebApplicationException(response);
        }

        return Collections.unmodifiableList(resultIds);
    }

    public List<Map<String, Object>> doIndexAllQuery(IDBI database,
            String type, String token, Long pageSize, boolean includeQuarantine)
            throws Exception {
        final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
        final SqlParamBindings bindings = new SqlParamBindings(true);

        final String querySql = tableHelper.getIndexAllQuery(type, token,
                pageSize, includeQuarantine);

        Response response = database
                .inTransaction(new TransactionCallback<Response>() {
                    @Override
                    public Response inTransaction(Handle handle,
                            TransactionStatus status) throws Exception {
                        try {
                            Query<Map<String, Object>> query = handle
                                    .createQuery(querySql);

                            bindings.bindToStatement(query);

                            for (Map<String, Object> r : query.list()) {
                                resultIds.add(r);
                            }

                            return null;
                        } catch (WebApplicationException e) {
                            return e.getResponse();
                        }
                    }
                });

        if (response != null) {
            throw new WebApplicationException(response);
        }

        return Collections.unmodifiableList(resultIds);
    }

    public boolean tableExists(IDBI database, final String type,
            final String indexName) {
        String tablename = tableHelper.getTableName(type, indexName);

        return JDBIHelper.tableExists(database, tableHelper.getPrefix(),
                tablename);
    }

    public void truncateTable(Handle handle, final String type,
            final String indexName) {
        String indexTableName = tableHelper.getTableName(type, indexName);
        handle.createStatement(tableHelper.getPrefix() + "truncate_table")
                .define("table_name", indexTableName).execute();
    }
}
