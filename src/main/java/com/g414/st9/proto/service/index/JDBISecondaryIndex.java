package com.g414.st9.proto.service.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;

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
import com.g414.st9.proto.service.validator.ValidationException;

public abstract class JDBISecondaryIndex {
    public void createTable(IDBI database, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
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
                handle.createStatement(
                        getIndexDefinition(type, indexName, schemaDefinition))
                        .execute();

                return null;
            }
        });
    }

    public void insertEntity(Handle handle, final Long id,
            final Map<String, Object> value, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        Update insert = handle.createStatement(getInsertStatement(type,
                indexName, schemaDefinition));

        for (IndexAttribute attr : schemaDefinition.getIndexMap()
                .get(indexName).getIndexAttributes()) {
            String attrName = attr.getName();
            if ("id".equals(attrName)) {
                insert.bind("id", id);
            } else {
                insert.bind(attrName, value.get(attrName));
            }
        }

        insert.execute();
    }

    public void updateEntity(Handle handle, final Long id,
            final Map<String, Object> value, final String type,
            final String indexName, final SchemaDefinition schemaDefinition) {
        Update update = handle.createStatement(getUpdateStatement(type,
                indexName, schemaDefinition));

        for (IndexAttribute attr : schemaDefinition.getIndexMap()
                .get(indexName).getIndexAttributes()) {
            String attrName = attr.getName();
            if ("id".equals(attrName)) {
                update.bind("id", id);
            } else {
                update.bind(attrName, value.get(attrName));
            }
        }

        update.execute();
    }

    public void deleteEntity(Handle handle, final Long id, final String type,
            final String indexName) {
        handle.createStatement(getDeleteStatement(type, indexName))
                .bind("id", id).execute();
    }

    protected static String getColumnName(String attributeName) {
        return "`_" + attributeName + "`";
    }

    protected static String getTableName(String type, String index) {
        return "`_i_" + type + "_" + index + "`";
    }

    protected static String getIndexName(String type, String index) {
        return "`_idx_" + type + "_" + index + "`";
    }

    protected abstract String getSqlType(AttributeType type);

    public String getTableDefinition(String type, String indexName,
            SchemaDefinition schemaDefinition) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

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

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create index ");
        sqlBuilder.append(getIndexName(type, indexName));
        sqlBuilder.append(" on ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" (");

        Iterator<IndexAttribute> iter = indexDefinition.getIndexAttributes()
                .iterator();

        while (iter.hasNext()) {
            IndexAttribute column = iter.next();

            Attribute attribute = schemaDefinition.getAttributesMap().get(
                    column.getName());

            if (attribute == null && !column.getName().equals("id")) {
                throw new IllegalArgumentException("Unknown attribute : "
                        + column.getName());
            }

            sqlBuilder.append(getColumnName(column.getName()));
            sqlBuilder.append(" ");
            sqlBuilder.append(column.getSortOrder().toString());

            if (iter.hasNext()) {
                sqlBuilder.append(", ");
            }
        }

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

    public List<Long> doIndexQuery(IDBI database, String type,
            String indexName, List<QueryTerm> queryTerms,
            SchemaDefinition schemaDefinition) {
        final List<Long> resultIds = new ArrayList<Long>();
        final Map<String, Object> bindParams = new LinkedHashMap<String, Object>();

        final String querySql = getIndexQuery(type, indexName, queryTerms,
                schemaDefinition, bindParams);

        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus status)
                    throws Exception {
                Query<Map<String, Object>> query = handle.createQuery(querySql);

                for (Map.Entry<String, Object> entry : bindParams.entrySet()) {
                    query.bind(entry.getKey(), entry.getValue());
                }

                for (Map<String, Object> r : query.list()) {
                    resultIds.add(((Number) r.get("_id")).longValue());
                }

                return null;
            }
        });

        return Collections.unmodifiableList(resultIds);
    }

    public String getIndexQuery(String type, String indexName,
            List<QueryTerm> queryTerms, SchemaDefinition schemaDefinition,
            Map<String, Object> bindParams) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

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

                    bindParams.put("p" + param, transformed);

                    maybeParam = " :p" + param;
                    param += 1;
                }

                clauses.add(getColumnName(term.getField()) + " "
                        + getSqlOperator(term.getOperator(), term.getValue())
                        + maybeParam);
            }
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select `_id` from ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" where ");
        sqlBuilder.append(StringHelper.join(" AND ", clauses));

        return sqlBuilder.toString();
    }

    protected static Map<String, List<QueryTerm>> sortTerms(
            IndexDefinition indexDef, List<QueryTerm> terms) {
        Map<String, List<QueryTerm>> termMap = new LinkedHashMap<String, List<QueryTerm>>();

        for (QueryTerm term : terms) {
            String attrName = term.getField();

            if (!indexDef.getAttributeNames().contains(attrName)) {
                throw new ValidationException("'" + attrName + "' not in index");
            }
        }

        for (IndexAttribute attribute : indexDef.getIndexAttributes()) {
            String attrName = attribute.getName();

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
}
