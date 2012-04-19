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

    private static final Set<AttributeType> FULLTEXT_SUPPORTED_ATTRIBUTE_TYPES = ImmutableSet
            .<AttributeType> of(AttributeType.BOOLEAN, AttributeType.ENUM,
                    AttributeType.I8, AttributeType.I16, AttributeType.I32,
                    AttributeType.I64, AttributeType.U8, AttributeType.U16,
                    AttributeType.U32, AttributeType.U64,
                    AttributeType.REFERENCE, AttributeType.UTC_DATE_SECS,
                    AttributeType.UTF8_SMALLSTRING, AttributeType.UTF8_TEXT);

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

        if (schemaDefinition.getFulltexts().size() > 0) {
            if (schemaDefinition.getFulltexts().size() > 1
                    || !schemaDefinition.getFulltexts().get(0).getName()
                            .equals("fulltext")) {
                throw new ValidationException(
                        "Schema definition only supports one fulltext index named 'fulltext' at this time");
            }
        }

        for (FulltextDefinition fulltext : schemaDefinition.getFulltexts()) {
            validateFulltext(theAtts, fulltext);
        }
    }

    public void validateUpdate(SchemaDefinition oldSchema,
            SchemaDefinition newSchema) {
        //
        // "throw an exception if they're incompatible"
        //
        // TODO : validate enums
        // TODO : validate data types (more/less specific etc)
        //
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
                || "kind".equalsIgnoreCase(indexName)
                || "all".equalsIgnoreCase(indexName)) {
            throw new ValidationException(
                    "Index may not be named 'id', 'kind', or 'all'");
        }

        if (!VALID_NAME_PATTERN.matcher(indexName).matches()) {
            throw new ValidationException("Invalid index name : " + indexName);
        }

        Set<String> included = new HashSet<String>();

        boolean isUnique = index.isUnique();

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

            if (isUnique && att.isNullable()) {
                throw new ValidationException(
                        "Unique index attribute must not be nullable: "
                                + colName);
            }

            included.add(colName);
        }

        if (!index.isUnique() && !included.contains("id")) {
            throw new ValidationException(
                    "Non-unique index *must* include 'id' attribute");
        }

        if (index.isUnique() && included.contains("id")) {
            throw new ValidationException(
                    "Unique index *must not* include 'id' attribute");
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

    private void validateFulltext(Map<String, Attribute> theAtts,
            FulltextDefinition fulltext) {
        String fulltextName = fulltext.getName();

        if ("id".equalsIgnoreCase(fulltextName)
                || "kind".equalsIgnoreCase(fulltextName)) {
            throw new ValidationException(
                    "Fulltext may not be named 'id' or 'kind'");
        }

        if (!VALID_NAME_PATTERN.matcher(fulltextName).matches()) {
            throw new ValidationException("Invalid fulltext name : "
                    + fulltextName);
        }

        String parent = fulltext.getParentIdentifierAttribute();
        if (parent != null) {
            if (!theAtts.containsKey(parent)
                    || !theAtts.get(parent).getType()
                            .equals(AttributeType.REFERENCE)) {
                throw new ValidationException(
                        "Invalid fulltext parent (attribute must be REFERENCE type) : "
                                + fulltextName);
            }
        }

        String parentType = fulltext.getParentType();
        if ((parentType == null) != (parent == null)) {
            throw new ValidationException(
                    "Invalid fulltext parent (if parentType is present, parentIdentifierAttribute must be present) : "
                            + fulltextName);
        }

        Set<String> included = new HashSet<String>();

        for (FulltextAttribute column : fulltext.getFullTextAttributes()) {
            String colName = column.getName();

            if ("id".equals(colName) || "type".equals(colName)
                    || "kind".equals(colName)) {
                throw new ValidationException("Attribute '" + colName
                        + "' not allowed in fulltext");
            }

            if (!theAtts.containsKey(colName)) {
                throw new ValidationException("Unknown attribute name: "
                        + colName);
            }

            Attribute att = theAtts.get(colName);

            if (!FULLTEXT_SUPPORTED_ATTRIBUTE_TYPES.contains(att.getType())) {
                throw new ValidationException(
                        "Attribute type not supported in fulltext: "
                                + att.getType());
            }

            if (included.contains(colName)) {
                throw new ValidationException(
                        "Attribute appears more than once in fulltext: "
                                + colName);
            }

            included.add(colName);
        }
    }
}
