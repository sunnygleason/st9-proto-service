package com.g414.st9.proto.service.index;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;

import com.g414.hash.LongHash;
import com.g414.hash.impl.MurmurHash;
import com.g414.st9.proto.service.cache.KeyValueCache;
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
import com.g414.st9.proto.service.schema.IndexAttribute;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.sequence.SequenceService;
import com.g414.st9.proto.service.store.Key;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class SecondaryIndexTableHelper {
    private String prefix;
    private SqlTypeHelper typeHelper;
    private LongHash longHash;
    private SequenceService sequences;

    @Inject
    public SecondaryIndexTableHelper(@Named("db.prefix") String prefix,
            SqlTypeHelper typeHelper, SequenceService sequences) {
        this.prefix = prefix;
        this.typeHelper = typeHelper;
        this.longHash = new MurmurHash();
        this.sequences = sequences;
    }

    public String getPrefix() {
        return prefix;
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
                        getTableDefinition(type, indexName, schemaDefinition))
                        .execute();

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

    public String getInsertStatement(String type, String indexName,
            SchemaDefinition schemaDefinition, SqlParamBindings bindings) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        List<String> cols = new ArrayList<String>();
        List<String> params = new ArrayList<String>();
        Set<String> already = new HashSet<String>();

        cols.add(getColumnName("id"));
        params.add(bindings.bind("id", AttributeType.U64));
        already.add("id");

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            if (already.contains(attr.getName())) {
                continue;
            }

            cols.add(getColumnName(attr.getName()));
            params.add(bindings.bind(attr.getName(), schemaDefinition
                    .getAttributesMap().get(attr.getName()).getType()));
        }

        cols.add(typeHelper.quote("quarantined"));
        params.add(bindings.bind("quarantined", "N", AttributeType.CHAR_ONE));

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
            SchemaDefinition schemaDefinition, SqlParamBindings bindings) {
        IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
                indexName);

        List<String> sets = new ArrayList<String>();

        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            if ("id".equals(attr.getName())) {
                continue;
            }

            sets.add(getColumnName(attr.getName())
                    + " = "
                    + bindings.bind(attr.getName(), schemaDefinition
                            .getAttributesMap().get(attr.getName()).getType()));
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("update ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" set ");
        sqlBuilder.append(StringHelper.join(", ", sets));
        sqlBuilder.append(" where ");
        sqlBuilder.append(typeHelper.quote("_id"));
        sqlBuilder.append(" = ");
        sqlBuilder.append(bindings.bind("id", AttributeType.U64));

        return sqlBuilder.toString();
    }

    public String getDeleteStatement(String type, String indexName,
            SqlParamBindings bindings) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("delete from ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" where ");
        sqlBuilder.append(typeHelper.quote("_id"));
        sqlBuilder.append(" = ");
        sqlBuilder.append(bindings.bind("id", AttributeType.U64));

        return sqlBuilder.toString();
    }

    public String getQuarantineStatement(String type, String indexName,
            SqlParamBindings bindings, boolean isQuarantined) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("update ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" set ");
        sqlBuilder.append(typeHelper.quote("quarantined"));
        sqlBuilder.append(" = ");
        sqlBuilder.append(bindings.bind("is_quarantined", isQuarantined ? "Y"
                : "N", AttributeType.CHAR_ONE));
        sqlBuilder.append(" where ");
        sqlBuilder.append(typeHelper.quote("_id"));
        sqlBuilder.append(" = ");
        sqlBuilder.append(bindings.bind("id", AttributeType.U64));

        return sqlBuilder.toString();
    }

    public void truncateIndexTable(Handle handle, final String type,
            final String indexName) {
        String indexTableName = getTableName(type, indexName);
        handle.createStatement(prefix + "truncate_table")
                .define("table_name", indexTableName).execute();
    }

    public String getSqlOperator(QueryOperator operator, QueryValue value) {
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
        case IN:
            return "in";
        default:
            throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }

    public Object transformAttributeValue(Object value, IndexAttribute attr) {
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
                    || message.contains("duplicate entry")
                    || message
                            .contains("unique index or primary key violation");
        }

        return false;
    }

    public String getColumnName(String attributeName, boolean doQuote) {
        return doQuote ? getColumnName(attributeName) : "_" + attributeName;
    }

    public String getColumnName(String attributeName) {
        return typeHelper.quote("_" + attributeName + "");
    }

    public String getTableName(String type, String index) {
        try {
            Integer typeId = sequences.getTypeId(type, false, false);
            if (typeId == null) {
                return null;
            }

            return typeHelper.quote("_i_" + String.format("%04d", typeId)
                    + "__" + getIndexHexId(index));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getIndexName(String type, String index) {
        try {
            return typeHelper.quote("_idx_"
                    + String.format("%04d", sequences.getTypeId(type, false))
                    + "__" + getIndexHexId(index));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getIndexHexId(String index) {
        return String.format("%016x", longHash.getLongHashCode(index));
    }

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
        sqlBuilder.append(typeHelper.quote("_id"));
        sqlBuilder.append(" ");
        sqlBuilder.append(typeHelper.getSqlType(AttributeType.U64));
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
            sqlBuilder.append(typeHelper.getSqlType(attribute.getType()));
        }

        sqlBuilder.append(", ");
        sqlBuilder.append(typeHelper.quote("quarantined"));
        sqlBuilder.append(" ");
        sqlBuilder.append(typeHelper.getSqlType(AttributeType.CHAR_ONE));

        sqlBuilder.append(")");
        sqlBuilder.append(typeHelper.getTableOptions());

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

    public String getIndexQuery(String type, String indexName,
            List<QueryTerm> queryTerms, String token, Long pageSize,
            boolean includeQuarantine, SchemaDefinition schemaDefinition,
            SqlParamBindings bindings) throws Exception {
        IndexDefinition indexDef = schemaDefinition.getIndexMap()
                .get(indexName);

        if (indexDef == null) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST)
                    .entity("schema or index not found " + type + "."
                            + indexName).build());
        }

        return getIndexQuery(type, indexName, sortTerms(indexDef, queryTerms),
                token, pageSize, includeQuarantine, indexDef, schemaDefinition,
                new SchemaValidatorTransformer(schemaDefinition), bindings);

    }

    public String getIndexQuery(String type, String indexName,
            Map<String, List<QueryTerm>> termMap, String token, Long pageSize,
            boolean includeQuarantine, IndexDefinition indexDefinition,
            SchemaDefinition schemaDefinition,
            SchemaValidatorTransformer transformer, SqlParamBindings bindings)
            throws Exception {
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
                QueryOperator op = term.getOperator();

                if (op.equals(QueryOperator.IN)) {
                    List<QueryValue> valueList = term.getValueList()
                            .getValueList();

                    String sqlOperator = getSqlOperator(term.getOperator(),
                            valueList.get(0));

                    List<String> paramNames = new ArrayList<String>();

                    for (QueryValue value : valueList) {
                        String boundParam = bindParam(attribute,
                                schemaDefinition, transformer, bindings, param,
                                attrName, value);

                        if (boundParam != null) {
                            maybeParam = " " + boundParam;
                            paramNames.add(maybeParam);
                            param += 1;
                        } else {
                            maybeParam = "";
                        }
                    }

                    clauses.add(getColumnName(term.getField()) + " "
                            + sqlOperator + "("
                            + StringHelper.join(", ", paramNames) + ")");
                } else {
                    String boundParam = bindParam(attribute, schemaDefinition,
                            transformer, bindings, param, attrName,
                            term.getValue());

                    if (boundParam != null) {
                        maybeParam = " " + boundParam;
                        param += 1;
                    }

                    clauses.add(getColumnName(term.getField())
                            + " "
                            + getSqlOperator(term.getOperator(),
                                    term.getValue()) + maybeParam);
                }
            }
        }

        List<String> sortOrders = new ArrayList<String>();
        for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
            String colName = getColumnName(attr.getName());
            sortOrders.add(colName + " " + attr.getSortOrder().name());
        }

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select ");
        sqlBuilder.append(typeHelper.quote("_id"));
        sqlBuilder.append(" from ");
        sqlBuilder.append(getTableName(type, indexName));
        sqlBuilder.append(" where ");
        if (!includeQuarantine) {
            sqlBuilder.append(typeHelper.quote("quarantined"));
            sqlBuilder.append(" = 'N' AND ");
        }
        sqlBuilder.append(StringHelper.join(" AND ", clauses));
        sqlBuilder.append(" order by ");
        sqlBuilder.append(StringHelper.join(", ", sortOrders));
        sqlBuilder.append(" limit ");
        sqlBuilder.append(pageSize + 1L);
        sqlBuilder.append(" offset ");
        sqlBuilder.append(OpaquePaginationHelper.decodeOpaqueCursor(token));

        return sqlBuilder.toString();
    }

    public List<Map<String, Object>> doUniqueIndexQuery(IDBI database,
            KeyValueCache cache, String type, String indexName,
            Map<String, List<QueryTerm>> termMap, String token,
            IndexDefinition indexDefinition, SchemaDefinition schemaDefinition,
            SchemaValidatorTransformer transformer) throws Exception {
        if (token != null) {
            return null;
        }

        Set<String> foundAttrs = new HashSet<String>();
        for (Map.Entry<String, List<QueryTerm>> entry : termMap.entrySet()) {
            if (entry.getValue().size() != 1) {
                return null;
            }

            if (!QueryOperator.EQ.equals(entry.getValue().get(0).getOperator())) {
                return null;
            }

            foundAttrs.add(entry.getKey());
        }

        for (String attrName : indexDefinition.getAttributeNames()) {
            if (!foundAttrs.contains(attrName)) {
                return null;
            }
        }

        Map<String, Object> value = new LinkedHashMap<String, Object>();

        for (IndexAttribute attribute : indexDefinition.getIndexAttributes()) {
            String attrName = attribute.getName();
            QueryTerm term = termMap.get(attrName).get(0);
            Object termValue = term.getValue().getValue();
            value.put(attrName, transformer.transformValue(attrName, termValue));
        }

        String cacheKey = computeIndexKey(indexName, indexDefinition, value,
                transformer);
        byte[] cacheResult = cache.get(cacheKey);

        if (cacheResult != null) {
            String resultString = new String(cacheResult);

            return ImmutableList.<Map<String, Object>> of(ImmutableMap
                    .<String, Object> of("_id", Long.parseLong(resultString)));
        }

        final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
        final SqlParamBindings bindings = new SqlParamBindings(true);

        final String querySql = getIndexQuery(type, indexName, termMap, token,
                10L, false, indexDefinition, schemaDefinition, transformer,
                bindings);

        Response response = database
                .inTransaction(new TransactionCallback<Response>() {
                    @Override
                    public Response inTransaction(Handle handle,
                            TransactionStatus status) throws Exception {
                        try {
                            Query<Map<String, Object>> query = handle
                                    .createQuery(querySql);

                            bindings.bindToStatement(query);

                            List<Map<String, Object>> qList = query.list();
                            if (qList == null || qList.size() > 1) {
                                throw new RuntimeException(
                                        "Non-unique result set!");
                            }

                            if (qList.size() == 1) {
                                resultIds.add(query.first());
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

        if (resultIds.size() > 0) {
            cache.put(cacheKey, resultIds.get(0).get("_id").toString()
                    .getBytes());
        }

        return Collections.unmodifiableList(resultIds);
    }

    public String computeIndexKey(String indexName,
            IndexDefinition indexDefinition, Map<String, Object> value,
            SchemaValidatorTransformer transformer) {
        StringBuilder theKey = new StringBuilder();
        theKey.append("idx:");
        theKey.append(indexName);
        theKey.append(":");

        try {
            Iterator<IndexAttribute> indexAttrIter = indexDefinition
                    .getIndexAttributes().iterator();

            while (indexAttrIter.hasNext()) {
                IndexAttribute attr = indexAttrIter.next();

                String attrName = attr.getName();
                if ("id".equals(attrName)) {
                    continue;
                }

                Object attrValue = value.get(attrName);
                Object transformed = (attrValue == null) ? null
                        : transformAttributeValue(attrValue, attr);

                String attrValueString = (transformed != null) ? URLEncoder
                        .encode(transformed.toString(), "UTF-8") : "$";

                theKey.append(attrValueString);

                if (indexAttrIter.hasNext()) {
                    theKey.append("|");
                }
            }
        } catch (UnsupportedEncodingException shouldntHappen) {
            throw new RuntimeException(shouldntHappen);
        }

        return theKey.toString();
    }

    public Map<String, List<QueryTerm>> sortTerms(IndexDefinition indexDef,
            List<QueryTerm> terms) {
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

    private String bindParam(IndexAttribute attribute,
            SchemaDefinition schemaDefinition,
            SchemaValidatorTransformer transformer, SqlParamBindings bindings,
            int param, String attrName, QueryValue value) {
        if (!value.getValueType().equals(ValueType.NULL)) {
            Object instance = value.getValue();
            Object transformed = transformer.transformValue(attrName, instance);

            if (transformed != null) {
                transformed = transformed.toString();
            }

            if (attrName.equals("id")) {
                try {
                    transformed = Key.valueOf(transformed.toString()).getId();
                } catch (Exception e) {
                    throw new ValidationException("invalid id: '"
                            + instance.toString() + "'");
                }
            }

            return bindings.bind(
                    "p" + param,
                    transformAttributeValue(transformed, attribute),
                    "id".equals(attribute.getName()) ? AttributeType.U64
                            : schemaDefinition.getAttributesMap()
                                    .get(attribute.getName()).getType());
        }

        return null;
    }
}
