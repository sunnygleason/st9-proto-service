package com.g414.st9.proto.service.count;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;

import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.helper.JDBIHelper;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.schema.Attribute;
import com.g414.st9.proto.service.schema.CounterAttribute;
import com.g414.st9.proto.service.schema.CounterDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.validator.ValidationException;
import com.google.inject.Inject;

public class JDBICountService {
    @Inject
    private CountServiceTableHelper tableHelper;

    public void createTable(IDBI database, final String type,
            final String counterName, final SchemaDefinition schemaDefinition) {
        JDBIHelper.createTable(database, tableHelper.getTableDrop(type,
                counterName), tableHelper.getTableDefinition(type, counterName,
                schemaDefinition));
    }

    public boolean tableExists(IDBI database, final String type,
            final String counterName) {
        return JDBIHelper.tableExists(database, tableHelper.getPrefix(),
                tableHelper.getTableName(type, counterName));
    }

    public void dropTable(IDBI database, final String type,
            final String counterName) {
        JDBIHelper.dropTable(database,
                tableHelper.getTableDrop(type, counterName));
    }

    public void insertEntity(Handle handle, final Long id,
            final Map<String, Object> value, final String type,
            final String counterName, final SchemaDefinition schemaDefinition)
            throws Exception {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        possiblyInsertCounter(handle, value, type, counterName,
                schemaDefinition, counterDefinition);
        updateCounter(handle, type, value, counterName, schemaDefinition,
                counterDefinition, 1);
    }

    public void updateEntity(Handle handle, final Long id,
            final Map<String, Object> original,
            final Map<String, Object> value, final String type,
            final String counterName, final SchemaDefinition schemaDefinition)
            throws Exception {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        updateCounter(handle, type, original, counterName, schemaDefinition,
                counterDefinition, -1);
        possiblyDeleteCounter(handle, type, original, counterName,
                schemaDefinition, counterDefinition);

        possiblyInsertCounter(handle, value, type, counterName,
                schemaDefinition, counterDefinition);
        updateCounter(handle, type, value, counterName, schemaDefinition,
                counterDefinition, 1);
    }

    public void deleteEntity(Handle handle, final Long id, final String type,
            final Map<String, Object> original, final String counterName,
            final SchemaDefinition schemaDefinition) throws Exception {
        CounterDefinition counterDefinition = schemaDefinition.getCounterMap()
                .get(counterName);

        updateCounter(handle, type, original, counterName, schemaDefinition,
                counterDefinition, -1);
        possiblyDeleteCounter(handle, type, original, counterName,
                schemaDefinition, counterDefinition);
    }

    public void clear(Handle handle, Iterator<Map<String, Object>> schemas,
            boolean preserveSchema) throws Exception {
        while (schemas.hasNext()) {
            Map<String, Object> schema = schemas.next();
            if (schema == null) {
                continue;
            }

            String type = (String) schema.get("$type");

            if (schema != null && schema.get("counters") != null) {
                for (Map<String, Object> counter : (List<Map<String, Object>>) schema
                        .get("counters")) {
                    String counterName = (String) counter.get("name");

                    if (preserveSchema) {
                        tableHelper.truncateTable(handle, type, counterName);
                    } else {
                        JDBIHelper.dropTable(handle,
                                tableHelper.getTableDrop(type, counterName));
                    }
                }
            }
        }
    }

    public List<Map<String, Object>> doCounterQuery(IDBI database, String type,
            String counterName, List<QueryTerm> queryTerms, String token,
            Long pageSize, SchemaDefinition schemaDefinition) throws Exception {
        final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
        final Map<String, Object> bindParams = new LinkedHashMap<String, Object>();

        final CounterDefinition counterDefinition = schemaDefinition
                .getCounterMap().get(counterName);
        final Map<String, Attribute> attributeMap = schemaDefinition
                .getAttributesMap();

        final SchemaValidatorTransformer transformer = new SchemaValidatorTransformer(
                schemaDefinition);

        final String querySql = tableHelper.getCounterQuery(type, counterName,
                queryTerms, token, pageSize, schemaDefinition, bindParams);

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
                                Map<String, Object> row = new LinkedHashMap<String, Object>();

                                for (CounterAttribute attr : counterDefinition
                                        .getCounterAttributes()) {
                                    Attribute realAttr = attributeMap.get(attr
                                            .getName());
                                    String colName = tableHelper.getColumnName(
                                            attr.getName(), false);

                                    if (r.containsKey(colName)) {
                                        row.put(attr.getName(),
                                                getRowValue(realAttr,
                                                        r.get(colName)));
                                    }
                                }

                                Map<String, Object> untrans = transformer
                                        .untransform(row);

                                untrans.put("count", r.get("count"));

                                resultIds.add(untrans);
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

    public void truncateTable(Handle handle, final String type,
            final String counterName) {
        String counterTableName = tableHelper.getTableName(type, counterName);
        handle.createStatement(tableHelper.getPrefix() + "truncate_table")
                .define("table_name", counterTableName).execute();
    }

    private void possiblyInsertCounter(Handle handle,
            final Map<String, Object> value, final String type,
            final String counterName, final SchemaDefinition schemaDefinition,
            CounterDefinition counterDefinition) throws Exception {
        Update insert = handle.createStatement(tableHelper.getInsertStatement(
                type, counterName, schemaDefinition, value));

        bindCounterParams(insert, value, counterDefinition);

        insert.execute();
    }

    private void possiblyDeleteCounter(Handle handle, final String type,
            final Map<String, Object> value, final String counterName,
            final SchemaDefinition schemaDefinition, CounterDefinition def)
            throws Exception {
        Update delete = handle.createStatement(tableHelper.getDeleteStatement(
                type, counterName, schemaDefinition, value));

        bindCounterParams(delete, value, def);

        delete.execute();
    }

    private void updateCounter(Handle handle, final String type,
            final Map<String, Object> value, final String counterName,
            final SchemaDefinition schemaDefinition, CounterDefinition def,
            int delta) throws Exception {
        Update update = handle.createStatement(tableHelper.getUpdateStatement(
                type, counterName, schemaDefinition, value));
        update.bind("__delta", delta);

        bindCounterParams(update, value, def);

        update.execute();
    }

    private void bindCounterParams(Update update,
            final Map<String, Object> value, CounterDefinition counterDefinition)
            throws Exception {
        Map<String, Object> key = new LinkedHashMap<String, Object>();

        for (CounterAttribute attr : counterDefinition.getCounterAttributes()) {
            String attrName = attr.getName();
            Object attrValue = tableHelper.transformAttributeValue(
                    value.get(attrName), attr);

            key.put(attr.getName(), attrValue);
            update.bind(attrName, attrValue);
        }

        update.bind("__hashcode", Long.valueOf(EncodingHelper.getKeyHash(key)));
    }

    private Object getRowValue(Attribute attribute, Object rowValue)
            throws ValidationException {
        switch (attribute.getType()) {
        case ENUM:
            return Integer.valueOf(rowValue.toString());
        case BOOLEAN:
            return (Integer.valueOf(rowValue.toString()) == 1) ? true : false;
        case U8:
        case U16:
        case U32:
        case U64:
        case I8:
        case I16:
        case I32:
        case I64:
            return new BigInteger(rowValue.toString());
        case REFERENCE:
        case UTF8_SMALLSTRING:
            return rowValue.toString();
        default:
            throw new ValidationException("unsupported attribute type '"
                    + attribute.getType().name() + "'");
        }
    }
}
