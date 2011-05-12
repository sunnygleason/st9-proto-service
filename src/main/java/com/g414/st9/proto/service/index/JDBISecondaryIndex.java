package com.g414.st9.proto.service.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToCreateStatementException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.Attribute;
import com.g414.st9.proto.service.schema.AttributeType;
import com.g414.st9.proto.service.schema.IndexAttribute;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public abstract class JDBISecondaryIndex {
    @Inject
    @Named("db.prefix")
    private String prefix;

    public void createTable(IDBI database, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                handle.createStatement(getTableDrop(type, indexName)).execute();

                handle.createStatement(
                        getTableDefinition(type, indexName, schemaDefinition))
                        .execute();

                return null;
            }
        });
    }

    public void createIndex(IDBI database, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                try {
                    handle.createStatement(prefix + "drop_index")
                            .define("table_name", getTableName(type, indexName))
                            .define("index_name", getIndexName(type, indexName))
                            .execute();
                } catch (UnableToExecuteStatementException ok) {
                    // expected case in mysql - this is just best-effort anyway
                }

                handle.createStatement(
                        getIndexDefinition(type, indexName, schemaDefinition))
                        .execute();

                return null;
            }
        });
    }

    public void dropTableAndIndex(Handle handle, final String type,
            final String indexName) {
        handle.createStatement(getTableDrop(type, indexName)).execute();

        try {
            handle.createStatement(prefix + "drop_index")
                    .define("table_name", getTableName(type, indexName))
                    .define("index_name", getIndexName(type, indexName))
                    .execute();
        } catch (UnableToExecuteStatementException ok) {
            // expected case in mysql - this is just best-effort anyway
        }
    }

    public void dropTableAndIndex(IDBI database, final String type,
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
        Update insert = handle.createStatement(getInsertStatement(type,
                indexName, schemaDefinition));

        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            String attrName = attr.getName();
            if ("id".equals(attrName)) {
                insert.bind("id", id);
            } else {
                insert.bind(attrName,
                        transformAttributeValue(value.get(attrName), attr));
            }
        }

        try {
            insert.execute();
        } catch (UnableToExecuteStatementException e) {
            if (isConstraintViolation(e)) {
                throw new WebApplicationException(Response
                        .status(Status.CONFLICT)
                        .entity("unique index constraint violation").build());
            } else {
                throw e;
            }
        }
    }

    public void updateEntity(Handle handle, final Long id,
            final Map<String, Object> value, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        Update update = handle.createStatement(getUpdateStatement(type,
                indexName, schemaDefinition));

        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            String attrName = attr.getName();
            if ("id".equals(attrName)) {
                update.bind("id", id);
            } else {
                update.bind(attrName,
                        transformAttributeValue(value.get(attrName), attr));
            }
        }

        try {
            update.execute();
        } catch (UnableToExecuteStatementException e) {
            if (isConstraintViolation(e)) {
                throw new WebApplicationException(Response
                        .status(Status.CONFLICT)
                        .entity("unique index constraint violation").build());
            } else {
                throw e;
            }
        }
    }

    public void deleteEntity(Handle handle, final Long id, final String type,
            final String indexName) {
        handle.createStatement(getDeleteStatement(type, indexName))
                .bind("id", id).execute();
    }

    public void rebuildIndexes(Handle handle, String type,
            SchemaDefinition schemaDefinition,
            Iterator<Map<String, Object>> instances) throws Exception {
        for (IndexDefinition index : schemaDefinition.getIndexes()) {
            this.truncateIndexTable(handle, type, index.getName());
        }

        SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                schemaDefinition);

        while (instances.hasNext()) {
            Map<String, Object> notTransformed = instances.next();
            Map<String, Object> instance = transformer
                    .validateTransform(notTransformed);

            Key key = Key.valueOf((String) instance.get("id"));

            Boolean deleted = (Boolean) instance.get("$deleted");

            if (deleted != null && deleted.booleanValue()) {
                continue;
            }

            for (IndexDefinition index : schemaDefinition.getIndexes()) {
                String indexName = index.getName();

                this.insertEntity(handle, key.getId(), instance, type,
                        indexName, schemaDefinition);
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
                        this.truncateIndexTable(handle, type, indexName);
                    } else {
                        this.dropTableAndIndex(handle, type, indexName);
                    }
                }
            }
        }
    }

    protected static String getColumnName(String attributeName, boolean doQuote) {
        return doQuote ? getColumnName(attributeName) : "_" + attributeName;
    }

    protected static String getColumnName(String attributeName) {
        return "`_" + attributeName + "`";
    }

    protected static String getTableName(String type, String index) {
        return "`_i_" + type + "__" + index + "`";
    }

    protected static String getIndexName(String type, String index) {
        return "`_idx_" + type + "__" + index + "`";
    }

    protected abstract String getSqlType(AttributeType type);

    public String getTableDrop(String type, String indexName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("drop table if exists ");
        sqlBuilder.append(getTableName(type, indexName));

        return sqlBuilder.toString();
    }

    public String getIndexDrop(String type, String indexName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("drop index ");
        sqlBuilder.append(getIndexName(type, indexName));
        sqlBuilder.append(" on ");
        sqlBuilder.append(getTableName(type, indexName));

        return sqlBuilder.toString();
    }

    public String getTableDefinition(String type, String indexName,
            SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        if (indexDefinition == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + indexName).build());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create table if not exists ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" (");
        sqlBuilder.append("`_id` ");
        sqlBuilder.append(getSqlType(AttributeType.U64));
        sqlBuilder.append(" PRIMARY KEY");

        for (IndexAttribute column : indexDefinition.getIndexAttributes()) {
            Attribute attribute = schemaDefinition.getAttributesMap().get(
                    column.getName());
            if (attribute == null && !column.getName().equals("id")) {
                throw new IllegalArgumentException("Unknown attribute : "
                        + column.getName());
            }

            if (column.getName().equals("id")) {
                continue;
            }

            sqlBuilder.append(", ");
            sqlBuilder.append(getColumnName(column.getName()));
            sqlBuilder.append(" ");
            sqlBuilder.append(getSqlType(attribute.getType()));
        }

        sqlBuilder.append(")");

        return sqlBuilder.toString();
    }

    public String getIndexDefinition(String type, String indexName,
            SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        if (indexDefinition == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + indexName).build());
        }

        Iterator<IndexAttribute> iter = indexDefinition.getIndexAttributes()
                .iterator();

        List<String> colDefs = new ArrayList<String>();
        while (iter.hasNext()) {
            IndexAttribute column = iter.next();

            Attribute attribute = schemaDefinition.getAttributesMap().get(
                    column.getName());

            if (attribute == null && !column.getName().equals("id")) {
                throw new IllegalArgumentException("Unknown attribute : "
                        + column.getName());
            }

            if (indexDefinition.isUnique() && column.getName().equals("id")) {
                continue;
            }

            colDefs.add(getColumnName(column.getName()) + " "
                    + column.getSortOrder().toString());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create ");

        if (indexDefinition.isUnique()) {
            sqlBuilder.append("unique ");
        }

        sqlBuilder.append("index ");
        sqlBuilder.append(getIndexName(type, indexName));
        sqlBuilder.append(" on ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" (");
        sqlBuilder.append(StringHelper.join(", ", colDefs));
        sqlBuilder.append(")");

        return sqlBuilder.toString();
    }

    public String getInsertStatement(String type, String indexName,
            SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        List<String> cols = new ArrayList<String>();
        List<String> params = new ArrayList<String>();

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            cols.add(getColumnName(attr.getName()));
            params.add(":" + attr.getName());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("insert into ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" (");
        sqlBuilder.append(StringHelper.join(", ", cols));
        sqlBuilder.append(") values (");
        sqlBuilder.append(StringHelper.join(", ", params));
        sqlBuilder.append(")");

        return sqlBuilder.toString();
    }

    public String getUpdateStatement(String type, String indexName,
            SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        List<String> sets = new ArrayList<String>();

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            if ("id".equals(attr.getName())) {
                continue;
            }

            sets.add(getColumnName(attr.getName()) + " = :" + attr.getName());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("update ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" set ");
        sqlBuilder.append(StringHelper.join(", ", sets));
        sqlBuilder.append(" where `_id` = :id");

        return sqlBuilder.toString();
    }

    public String getDeleteStatement(String type, String indexName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("delete from ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" where `_id` = :id");

        return sqlBuilder.toString();
    }

    public List<Map<String, Object>> doIndexQuery(IDBI database, String type,
            String indexName, List<QueryTerm> queryTerms, String token,
            Long pageSize, SchemaDefinition schemaDefinition) throws Exception {
        final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
        final Map<String, Object> bindParams = new LinkedHashMap<String, Object>();

        final String querySql = getIndexQuery(type, indexName, queryTerms,
                token, pageSize, schemaDefinition, bindParams);

        Response response = database
                .inTransaction(new TransactionCallback<Response>() {
                    @Override
                    public Response inTransaction(Handle handle,
                            TransactionStatus status) throws Exception {
                        try {
                            Query<Map<String, Object>> query = handle
                                    .createQuery(querySql);

                            for (Map.Entry<String, Object> entry : bindParams
                                    .entrySet()) {
                                query.bind(entry.getKey(), entry.getValue());
                            }

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

    public String getIndexQuery(String type, String indexName,
            List<QueryTerm> queryTerms, String token, Long pageSize,
            SchemaDefinition schemaDefinition, Map<String, Object> bindParams)
            throws Exception {
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

        Map<String, List<QueryTerm>> termMap = sortTerms(indexDefinition,
                queryTerms);

        List<QueryTerm> firstTerm = termMap.get(indexDefinition
                .getIndexAttributes().get(0).getName());
        if (firstTerm == null || firstTerm.isEmpty()) {
            throw new ValidationException(
                    "missing query term for first attribute of index");
        }

        List<String> clauses = new ArrayList<String>();
        int param = 0;

        for (IndexAttribute attribute : indexDefinition.getIndexAttributes()) {
            String attrName = attribute.getName();
            List<QueryTerm> termList = termMap.get(attrName);

            if (termList == null || termList.isEmpty()) {
                continue;
            }

            for (QueryTerm term : termList) {
                String maybeParam = "";

                if (!term.getValue().getValueType().equals(ValueType.NULL)) {
                    Object instance = term.getValue().getValue();
                    Object transformed = transformer.transformValue(attrName,
                            instance);

                    bindParams.put("p" + param,
                            transformAttributeValue(transformed, attribute));

                    maybeParam = " :p" + param;
                    param += 1;
                }

                clauses.add(getColumnName(term.getField()) + " "
                        + getSqlOperator(term.getOperator(), term.getValue())
                        + maybeParam);
            }
        }

        List<String> sortOrders = new ArrayList<String>();
        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            String colName = getColumnName(attr.getName());
            sortOrders.add(colName + " " + attr.getSortOrder().name());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select `_id`");
        sqlBuilder.append(" from ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" where ");
        sqlBuilder.append(StringHelper.join(" AND ", clauses));
        sqlBuilder.append(" order by ");
        sqlBuilder.append(StringHelper.join(", ", sortOrders));
        sqlBuilder.append(" limit ");
        sqlBuilder.append(pageSize + 1L);
        sqlBuilder.append(" offset ");
        sqlBuilder.append(OpaquePaginationHelper.decodeOpaqueCursor(token));

        return sqlBuilder.toString();
    }

    public boolean indexExists(IDBI database, final String type,
            final String indexName) {
        return database.inTransaction(new TransactionCallback<Boolean>() {
            @Override
            public Boolean inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                String indexTableName = getTableName(type, indexName);
                try {
                    handle.createStatement(prefix + "table_exists")
                            .define("table_name", indexTableName).execute();

                    return true;
                } catch (UnableToExecuteStatementException e) {
                    // expected in missing case
                } catch (UnableToCreateStatementException e) {
                    // expected in missing case
                }

                return false;
            }
        });
    }

    public void truncateIndexTable(Handle handle, final String type,
            final String indexName) {
        String indexTableName = getTableName(type, indexName);
        handle.createStatement(prefix + "truncate_table")
                .define("table_name", indexTableName).execute();
    }

    protected static Map<String, List<QueryTerm>> sortTerms(
            IndexDefinition indexDef, List<QueryTerm> terms) {
        Map<String, List<QueryTerm>> termMap = new LinkedHashMap<String, List<QueryTerm>>();

        Set<String> termFields = new HashSet<String>();

        for (QueryTerm term : terms) {
            String attrName = term.getField();
            termFields.add(attrName);

            if (!indexDef.getAttributeNames().contains(attrName)) {
                throw new ValidationException("'" + attrName + "' not in index");
            }
        }

        for (IndexAttribute attribute : indexDef.getIndexAttributes()) {
            String attrName = attribute.getName();

            if (indexDef.isUnique() && !attrName.equals("id")
                    && !termFields.contains(attrName)) {
                throw new ValidationException(
                        "unique index query must specify all fields");
            }

            for (QueryTerm term : terms) {
                if (term.getField().equals(attrName)) {
                    List<QueryTerm> attrTerms = termMap.get(attrName);
                    if (attrTerms == null) {
                        attrTerms = new ArrayList<QueryTerm>();
                        termMap.put(attrName, attrTerms);
                    }

                    attrTerms.add(term);
                }
            }
        }

        return termMap;
    }

    protected String getSqlOperator(QueryOperator operator, QueryValue value) {
        switch (operator) {
        case EQ:
            return (value.getValueType().equals(ValueType.NULL)) ? "is null"
                    : "=";
        case NE:
            return (value.getValueType().equals(ValueType.NULL)) ? "is not null"
                    : "<>";
        case GT:
            return ">";
        case GE:
            return ">=";
        case LT:
            return "<";
        case LE:
            return "<=";
        default:
            throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }

    protected Object transformAttributeValue(Object value, IndexAttribute attr) {
        Object toBind = value;

        if (toBind != null) {
            switch (attr.getTransform()) {
            case UPPERCASE:
                toBind = toBind.toString().toUpperCase();
                break;
            case LOWERCASE:
                toBind = toBind.toString().toLowerCase();
                break;
            default:
                break;
            }
        }

        return toBind;
    }

    protected boolean isConstraintViolation(UnableToExecuteStatementException e) {
        if (e.getCause() != null) {
            String message = e.getCause().getMessage().toLowerCase();

            return message.contains("constraint violation")
                    || message.contains("duplicate entry");
        }

        return false;
    }
}
