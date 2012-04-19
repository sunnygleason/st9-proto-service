package com.g414.st9.proto.service.schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.g414.st9.proto.service.validator.ValidationException;
import com.google.common.collect.ImmutableSet;

public class SchemaValidationHelper {
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

    public static void validateAttribute(Attribute attribute) {
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

        if (attribute.getType().equals(AttributeType.ENUM)) {
            List<String> values = attribute.getValues();
            if (values == null || values.size() < 1) {
                throw new ValidationException(
                        "Invalid enum attribute (contains no values) : "
                                + attrName);
            }
        }
    }

    public static void validateIndex(Map<String, Attribute> theAtts,
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

    public static void validateCounter(Map<String, Attribute> theAtts,
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

    public static void validateFulltext(Map<String, Attribute> theAtts,
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

    public static void validateAttributeUpgrade(Attribute oldAttr,
            Attribute newAttr) {
        String name = oldAttr.getName();
        AttributeType oldType = oldAttr.getType();
        AttributeType newType = (newAttr == null) ? null : newAttr.getType();

        switch (oldType) {
        case ANY:
        case ARRAY:
        case MAP:
        case BOOLEAN:
        case CHAR_ONE:
            assertAttributeTypeOneOf(name, newType, null, oldType);
            break;

        case REFERENCE:
            assertAttributeTypeOneOf(name, newType, AttributeType.REFERENCE);
            break;

        case ENUM:
            assertAttributeTypeOneOf(name, newType, AttributeType.ENUM);
            assertEnumAttributeCompatible(oldAttr, newAttr);
            break;

        case I8:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.I8,
                    AttributeType.I16, AttributeType.I32, AttributeType.I64);
            break;
        case I16:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.I16,
                    AttributeType.I32, AttributeType.I64);
            break;
        case I32:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.I32,
                    AttributeType.I64);
            break;
        case I64:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.I64);
            break;

        case U8:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.U8,
                    AttributeType.U16, AttributeType.U32, AttributeType.U64);
            break;
        case U16:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.U16,
                    AttributeType.U32, AttributeType.U64);
            break;
        case U32:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.U32,
                    AttributeType.U64);
            break;
        case U64:
            assertAttributeTypeOneOf(name, newType, null, AttributeType.U64);
            break;

        case UTC_DATE_SECS:
            assertAttributeTypeOneOf(name, newType, null,
                    AttributeType.UTC_DATE_SECS, AttributeType.I64);
            break;

        case UTF8_SMALLSTRING:
            assertAttributeTypeOneOf(name, newType, null,
                    AttributeType.UTF8_SMALLSTRING, AttributeType.UTF8_TEXT);
            break;

        case UTF8_TEXT:
            assertAttributeTypeOneOf(name, newType, null,
                    AttributeType.UTF8_TEXT);
            break;
        }
    }

    public static void assertEnumAttributeCompatible(Attribute oldAttr,
            Attribute newAttr) {
        if (newAttr == null) {
            throw new ValidationException("enum attributes, such as '"
                    + oldAttr + "' may not be removed");
        }

        List<String> newValues = newAttr.getValues();
        List<String> oldValues = oldAttr.getValues();

        if (newValues == null || oldValues == null) {
            throw new ValidationException("enum attribute '"
                    + oldAttr.getName() + "' has no values");
        }

        if (newValues.size() < oldValues.size()) {
            throw new ValidationException("may not remove enum values from '"
                    + oldAttr.getName() + "', " + newValues
                    + " has fewer items than " + oldValues);
        }

        for (int i = 0; i < oldValues.size(); i++) {
            if (!oldValues.get(i).equals(newValues.get(i))) {
                throw new ValidationException(
                        "illegal enum values update for '" + oldAttr.getName()
                                + "', " + newValues
                                + " updates in the front or middle of "
                                + oldValues);
            }
        }
    }

    public static void assertAttributeTypeOneOf(String attrName,
            AttributeType theType, AttributeType... possibles) {
        for (AttributeType candidate : possibles) {
            if (theType == null && candidate == null) {
                return;
            }

            if (theType != null && theType.equals(candidate)) {
                return;
            }
        }

        throw new ValidationException("attribute '" + attrName
                + "' updated to '" + theType + "', must be one of "
                + Arrays.asList(possibles));
    }
}
