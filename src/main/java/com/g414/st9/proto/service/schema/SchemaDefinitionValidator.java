package com.g414.st9.proto.service.schema;

import java.util.LinkedHashMap;
import java.util.Map;

import com.g414.st9.proto.service.validator.ValidationException;

public class SchemaDefinitionValidator {

    public void validate(SchemaDefinition schemaDefinition)
            throws ValidationException {
        Map<String, Attribute> theAtts = new LinkedHashMap<String, Attribute>();

        for (Attribute attribute : schemaDefinition.getAttributes()) {
            SchemaValidationHelper.validateAttribute(attribute);
            theAtts.put(attribute.getName(), attribute);
        }

        for (IndexDefinition index : schemaDefinition.getIndexes()) {
            SchemaValidationHelper.validateIndex(theAtts, index);
        }

        for (CounterDefinition counter : schemaDefinition.getCounters()) {
            SchemaValidationHelper.validateCounter(theAtts, counter);
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
            SchemaValidationHelper.validateFulltext(theAtts, fulltext);
        }
    }

    public void validateUpgrade(SchemaDefinition oldSchema,
            SchemaDefinition newSchema) {
        Map<String, Attribute> newAttrs = newSchema.getAttributesMap();
        Map<String, Attribute> oldAttrs = oldSchema.getAttributesMap();

        // validate existing attributes
        for (Attribute oldAttr : oldAttrs.values()) {
            Attribute newAttr = newAttrs.get(oldAttr.getName());
            SchemaValidationHelper.validateAttributeUpgrade(oldAttr, newAttr);
        }

        // tricky note: 'new' attributes may already exist in the entities;
        // if those entities are incompatible, they will become irretrievable,
        // and related index, counter, or fulltext rebuilds may fail
    }
}
