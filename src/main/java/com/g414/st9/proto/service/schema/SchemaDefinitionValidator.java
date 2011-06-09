package com.g414.st9.proto.service.schema;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.g414.st9.proto.service.validator.ValidationException;
import com.google.common.collect.ImmutableSet;

public class SchemaDefinitionValidator {
    private static final Pattern VALID_NAME_PATTERN = Pattern
            .compile("[a-zA-Z0-9_]+");

    private static final Set<AttributeType> INDEX_SUPPORTED_ATTRIBUTE_TYPES = ImmutableSet
            .<AttributeType> of(AttributeType.BOOLEAN, AttributeType.ENUM,
                    AttributeType.I8, AttributeType.I16, AttributeType.I32,
                    AttributeType.I64, AttributeType.U8, AttributeType.U16,
                    AttributeType.U32, AttributeType.U64,
                    AttributeType.REFERENCE, AttributeType.UTC_DATE_SECS,
                    AttributeType.UTF8_SMALLSTRING);

    public void validate(SchemaDefinition schemaDefinition)
            throws ValidationException {
        Map<String, Attribute> theAtts = new LinkedHashMap<String, Attribute>();

        for (Attribute attribute : schemaDefinition.getAttributes()) {
            validateAttribute(attribute);
            theAtts.put(attribute.getName(), attribute);
        }

        for (IndexDefinition index : schemaDefinition.getIndexes()) {
            validateIndex(theAtts, index);
        }

        for (CounterDefinition counter : schemaDefinition.getCounters()) {
            validateCounter(theAtts, counter);
        }
    }

    private void validateAttribute(Attribute attribute) {
        String attrName = attribute.getName();

        if ("id".equalsIgnoreCase(attrName)
                || "kind".equalsIgnoreCase(attrName)) {
            throw new ValidationException(
                    "Must not include 'id' or 'kind' attributes in schema");
        }

        if (!VALID_NAME_PATTERN.matcher(attrName).matches()) {
            throw new ValidationException("Invalid attribute name : "
                    + attrName);
        }
    }

    private void validateIndex(Map<String, Attribute> theAtts,
            IndexDefinition index) {
        String indexName = index.getName();

        if ("id".equalsIgnoreCase(indexName)
                || "kind".equalsIgnoreCase(indexName)) {
            throw new ValidationException(
                    "Index may not be named 'id' or 'kind'");
        }

        if (!VALID_NAME_PATTERN.matcher(indexName).matches()) {
            throw new ValidationException("Invalid index name : " + indexName);
        }

        Set<String> included = new HashSet<String>();

        for (IndexAttribute column : index.getIndexAttributes()) {
            String colName = column.getName();

            if (!theAtts.containsKey(colName) && !colName.equals("id")) {
                throw new ValidationException("Unknown attribute name: "
                        + colName);
            }

            Attribute att = theAtts.get(colName);
            if (att == null && "id".equals(colName)) {
                att = new Attribute("id", AttributeType.I64, null, null, null,
                        false);
            }

            if (!INDEX_SUPPORTED_ATTRIBUTE_TYPES.contains(att.getType())) {
                throw new ValidationException(
                        "Attribute type not supported in index: "
                                + att.getType());
            }

            if (included.contains(colName)) {
                throw new ValidationException(
                        "Attribute appears more than once in index: " + colName);
            }

            included.add(colName);
        }

        if (!included.contains("id")) {
            throw new ValidationException("Index must include 'id' attribute");
        }
    }

    private void validateCounter(Map<String, Attribute> theAtts,
            CounterDefinition counter) {
        String counterName = counter.getName();

        if ("id".equalsIgnoreCase(counterName)
                || "kind".equalsIgnoreCase(counterName)) {
            throw new ValidationException(
                    "Counter may not be named 'id' or 'kind'");
        }

        if (!VALID_NAME_PATTERN.matcher(counterName).matches()) {
            throw new ValidationException("Invalid counter name : "
                    + counterName);
        }

        Set<String> included = new HashSet<String>();

        for (CounterAttribute column : counter.getCounterAttributes()) {
            String colName = column.getName();

            if ("id".equals(colName) || "type".equals(colName)
                    || "kind".equals(colName)) {
                throw new ValidationException("Attribute '" + colName
                        + "' not allowed in counter");
            }

            if (!theAtts.containsKey(colName)) {
                throw new ValidationException("Unknown attribute name: "
                        + colName);
            }

            Attribute att = theAtts.get(colName);

            if (!INDEX_SUPPORTED_ATTRIBUTE_TYPES.contains(att.getType())) {
                throw new ValidationException(
                        "Attribute type not supported in counter: "
                                + att.getType());
            }

            if (att.isNullable()) {
                throw new ValidationException(
                        "Nullable attribute not supported in counter: "
                                + att.getName());
            }

            if (included.contains(colName)) {
                throw new ValidationException(
                        "Attribute appears more than once in counter: "
                                + colName);
            }

            included.add(colName);
        }
    }
}
