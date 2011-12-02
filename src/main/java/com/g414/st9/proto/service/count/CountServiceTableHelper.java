package com.g414.st9.proto.service.count;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import com.g414.hash.LongHash;
import com.g414.hash.impl.MurmurHash;
import com.g414.st9.proto.service.helper.OpaquePaginationHelper;
import com.g414.st9.proto.service.helper.SqlParamBindings;
import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.helper.StringHelper;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.Attribute;
import com.g414.st9.proto.service.schema.AttributeType;
import com.g414.st9.proto.service.schema.CounterAttribute;
import com.g414.st9.proto.service.schema.CounterDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.sequence.SequenceService;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class CountServiceTableHelper {
    private String prefix;
    private SqlTypeHelper typeHelper;
    private LongHash longHash;
    private SequenceService sequences;

    @Inject
    public CountServiceTableHelper(@Named("db.prefix") String prefix,
            SqlTypeHelper typeHelper, SequenceService sequences) {
        this.prefix = prefix;
        this.typeHelper = typeHelper;
        this.longHash = new MurmurHash();
        this.sequences = sequences;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getInsertStatement(String type, String counterName,
            SchemaDefinition schemaDefinition, Map<String, Object> value,
            SqlParamBindings bindings) {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        List<String> cols = new ArrayList<String>();
        List<String> params = new ArrayList<String>();

        for (CounterAttribute attr : counterDefinition.getCounterAttributes()) {
            cols.add(getColumnName(attr.getName()));
            params.add(bindings.bind(
                    attr.getName(),
                    "id".equals(attr.getName()) ? AttributeType.U64
                            : schemaDefinition.getAttributesMap()
                                    .get(attr.getName()).getType()));
        }

        cols.add(typeHelper.quote("__hashcode"));
        params.add(bindings.bind("__hashcode", AttributeType.I64));

        cols.add(typeHelper.quote("__count"));
        params.add("0");

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(typeHelper.getInsertIgnore());
        sqlBuilder.append(" into ");
        sqlBuilder.append(getTableName(type, counterName));
        sqlBuilder.append(" (");
        sqlBuilder.append(StringHelper.join(", ", cols));
        sqlBuilder.append(") values (");
        sqlBuilder.append(StringHelper.join(", ", params));
        sqlBuilder.append(")");

        return sqlBuilder.toString();
    }

    public String getUpdateStatement(String type, String counterName,
            SchemaDefinition schemaDefinition, Map<String, Object> value,
            SqlParamBindings bindings) {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("update ");
        sqlBuilder.append(getTableName(type, counterName));
        sqlBuilder.append(" set ");
        sqlBuilder.append(typeHelper.quote("__count"));
        sqlBuilder.append(" = ");
        sqlBuilder.append(typeHelper.quote("__count"));
        sqlBuilder.append(" + ");
        sqlBuilder.append(bindings.bind("__delta", AttributeType.U64));
        sqlBuilder.append(" where ");
        sqlBuilder.append(StringHelper.join(
                " and ",
                getCounterMatchClauses(schemaDefinition, counterDefinition,
                        value, bindings)));

        return sqlBuilder.toString();
    }

    public String getDeleteStatement(String type, String counterName,
            SchemaDefinition schemaDefinition, Map<String, Object> value,
            SqlParamBindings bindings) {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("delete from ");
        sqlBuilder.append(getTableName(type, counterName));
        sqlBuilder.append(" where ");
        sqlBuilder.append(typeHelper.quote("__count"));
        sqlBuilder.append(" = 0");

        List<String> where = getCounterMatchClauses(schemaDefinition,
                counterDefinition, value, bindings);
        if (!where.isEmpty()) {
            sqlBuilder.append(" and ");
            sqlBuilder.append(StringHelper.join(" and ", where));
        }

        return sqlBuilder.toString();
    }

    private List<String> getCounterMatchClauses(
            SchemaDefinition schemaDefinition,
            CounterDefinition counterDefinition, Map<String, Object> value,
            SqlParamBindings bindings) {
        List<String> sets = new ArrayList<String>();

        sets.add(typeHelper.quote("__hashcode") + " = "
                + bindings.bind("__hashcode", AttributeType.I64));

        for (CounterAttribute attr : counterDefinition.getCounterAttributes()) {
            if (value.get(attr.getName()) != null) {
                sets.add(getColumnName(attr.getName())
                        + " = "
                        + bindings.bind(attr.getName(), schemaDefinition
                                .getAttributesMap().get(attr.getName())
                                .getType()));
            } else {
                sets.add(getColumnName(attr.getName()) + " is null");
            }
        }

        return sets;
    }

    public void truncateTable(Handle handle, final String type,
            final String counterName) {
        String indexTableName = getTableName(type, counterName);
        handle.createStatement(prefix + "truncate_table")
                .define("table_name", indexTableName).execute();
    }

    public String getSqlOperator(QueryOperator operator, QueryValue value) {
        switch (operator) {
        case EQ:
            return (value.getValueType().equals(ValueType.NULL)) ? "is null"
                    : "=";
        default:
            throw new IllegalArgumentException("Unsupported operator: "
                    + operator);
        }
    }

    public Object transformAttributeValue(Object value, CounterAttribute attr) {
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

    public boolean isConstraintViolation(UnableToExecuteStatementException e) {
        if (e.getCause() != null) {
            String message = e.getCause().getMessage().toLowerCase();

            return message.contains("constraint violation")
                    || message.contains("duplicate entry");
        }

        return false;
    }

    public String getColumnName(String attributeName, boolean doQuote) {
        return doQuote ? getColumnName(attributeName) : "_" + attributeName;
    }

    public String getColumnName(String attributeName) {
        return typeHelper.quote("_" + attributeName);
    }

    public String getTableName(String type, String index) {
        try {
            Integer typeId = sequences.getTypeId(type, false, false);
            if (typeId == null) {
                return null;
            }

            return typeHelper.quote("_c_" + String.format("%04d", typeId)
                    + "__" + getCounterHexId(index));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getCounterHexId(String index) {
        return String.format("%016x", longHash.getLongHashCode(index));
    }

    public String getTableDrop(String type, String indexName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("drop table if exists ");
        sqlBuilder.append(getTableName(type, indexName));

        return sqlBuilder.toString();
    }

    public String getTableDefinition(String type, String counterName,
            SchemaDefinition schemaDefinition) {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        if (counterDefinition == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or counter not found " + type + "."
                            + counterName).build());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create table if not exists ");
        sqlBuilder.append(getTableName(type, counterName));
        sqlBuilder.append(" (");

        List<String> pkCols = new ArrayList<String>();

        for (CounterAttribute column : counterDefinition.getCounterAttributes()) {
            Attribute attribute = schemaDefinition.getAttributesMap().get(
                    column.getName());
            String colName = getColumnName(column.getName());

            sqlBuilder.append(colName);
            sqlBuilder.append(" ");
            sqlBuilder.append(typeHelper.getSqlType(attribute.getType()));
            sqlBuilder.append(", ");

            pkCols.add(colName);
        }

        sqlBuilder.append(typeHelper.quote("__hashcode"));
        sqlBuilder.append(" ");
        sqlBuilder.append(typeHelper.getSqlType(AttributeType.U64));
        sqlBuilder.append(" not null, ");
        sqlBuilder.append(typeHelper.quote("__count"));
        sqlBuilder.append(" ");
        sqlBuilder.append(typeHelper.getSqlType(AttributeType.U64));
        sqlBuilder.append(", PRIMARY KEY(");
        sqlBuilder.append(StringHelper.join(", ", pkCols));
        sqlBuilder.append("), UNIQUE(");
        sqlBuilder.append(typeHelper.quote("__hashcode"));
        sqlBuilder.append("))");
        sqlBuilder.append(typeHelper.getTableOptions());

        return sqlBuilder.toString();
    }

    public List<Map<String, Object>> doIndexQuery(IDBI database, String type,
            String counterName, List<QueryTerm> queryTerms, String token,
            Long pageSize, SchemaDefinition schemaDefinition) throws Exception {
        final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
        final SqlParamBindings bindings = new SqlParamBindings(true);

        final String querySql = getCounterQuery(type, counterName, queryTerms,
                token, pageSize, schemaDefinition, bindings);

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

    public String getCounterQuery(String type, String counterName,
            List<QueryTerm> queryTerms, String token, Long pageSize,
            SchemaDefinition schemaDefinition, SqlParamBindings bindings)
            throws Exception {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        if (counterDefinition == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or counter not found " + type + "."
                            + counterName).build());
        }

        final SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                schemaDefinition);

        Map<String, List<QueryTerm>> termMap = sortTerms(counterDefinition,
                queryTerms);

        List<String> clauses = new ArrayList<String>();
        int param = 0;

        List<String> outCols = new ArrayList<String>();

        for (CounterAttribute attribute : counterDefinition
                .getCounterAttributes()) {
            String attrName = attribute.getName();
            List<QueryTerm> termList = termMap.get(attrName);

            if (termList == null || termList.isEmpty()) {
                outCols.add(getColumnName(attrName));
                continue;
            }

            for (QueryTerm term : termList) {
                String maybeParam = "";

                if (!term.getValue().getValueType().equals(ValueType.NULL)) {
                    Object instance = term.getValue().getValue();
                    Object transformed = transformer.transformValue(attrName,
                            instance);

                    if (transformed != null) {
                        transformed = transformed.toString();
                    }

                    maybeParam = " "
                            + bindings.bind(
                                    "p" + param,
                                    transformAttributeValue(transformed,
                                            attribute), schemaDefinition
                                            .getAttributesMap().get(attrName)
                                            .getType());

                    param += 1;
                }

                clauses.add(getColumnName(term.getField()) + " "
                        + getSqlOperator(term.getOperator(), term.getValue())
                        + maybeParam);
            }
        }

        outCols.add(typeHelper.quote("__count"));

        List<String> sortOrders = new ArrayList<String>();
        for (CounterAttribute attr : counterDefinition.getCounterAttributes()) {
            String colName = getColumnName(attr.getName());
            sortOrders.add(colName + " " + attr.getSortOrder().name());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ");
        sqlBuilder.append(StringHelper.join(", ", outCols));
        sqlBuilder.append(" from ");
        sqlBuilder.append(getTableName(type, counterName));

        if (clauses.size() > 0) {
            sqlBuilder.append(" where ");
            sqlBuilder.append(StringHelper.join(" AND ", clauses));
        }

        sqlBuilder.append(" order by ");
        sqlBuilder.append(StringHelper.join(", ", sortOrders));
        sqlBuilder.append(" limit ");
        sqlBuilder.append(pageSize + 1L);
        sqlBuilder.append(" offset ");
        sqlBuilder.append(OpaquePaginationHelper.decodeOpaqueCursor(token));

        return sqlBuilder.toString();
    }

    private static Map<String, List<QueryTerm>> sortTerms(
            CounterDefinition counterDef, List<QueryTerm> terms) {
        Map<String, List<QueryTerm>> termMap = new LinkedHashMap<String, List<QueryTerm>>();

        Set<String> termFields = new HashSet<String>();

        for (QueryTerm term : terms) {
            String attrName = term.getField();
            termFields.add(attrName);

            if (!counterDef.getAttributeNames().contains(attrName)) {
                throw new ValidationException("'" + attrName + "' not in index");
            }
        }

        for (CounterAttribute attribute : counterDef.getCounterAttributes()) {
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
}
