package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.g414.st9.proto.service.validator.ReferenceValidator;
import com.g414.st9.proto.service.validator.ValidationException;
import com.g414.st9.proto.service.validator.ValidatorTransformer;

/**
 * Initial implementation of schema validation and transformation for KV
 * storage.
 */
public class SchemaValidatorTransformer implements
        ValidatorTransformer<Map<String, Object>, Map<String, Object>> {
    private final Map<String, ValidatorTransformer<?, ?>> attributeValidators;
    private final Map<String, Attribute> attributesByName;

    public SchemaValidatorTransformer(SchemaDefinition schemaDefinition) {
        Validators validators = new Validators();

        Map<String, Attribute> newAttributesByName = new LinkedHashMap<String, Attribute>();

        newAttributesByName.put("id", new Attribute("id",
                AttributeType.REFERENCE, null, null, null, Boolean.TRUE));

        for (Attribute attribute : schemaDefinition.getAttributes()) {
            newAttributesByName.put(attribute.getName(), attribute);
        }

        this.attributesByName = Collections
                .unmodifiableMap(newAttributesByName);

        Map<String, ValidatorTransformer<?, ?>> newAttributeValidators = new LinkedHashMap<String, ValidatorTransformer<?, ?>>();

        newAttributeValidators.put("id", new ReferenceValidator("id"));

        for (Attribute attribute : schemaDefinition.getAttributes()) {
            newAttributeValidators.put(attribute.getName(),
                    validators.validatorFor(attribute));
        }

        this.attributeValidators = Collections
                .unmodifiableMap(newAttributeValidators);
    }

    @Override
    public Map<String, Object> validateTransform(Map<String, Object> instance)
            throws ValidationException {
        if (instance == null) {
            throw new ValidationException("instance must not be null");
        }

        Map<String, Object> mindlessClone = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, ValidatorTransformer<?, ?>> e : attributeValidators
                .entrySet()) {
            String attrName = e.getKey();
            ValidatorTransformer validator = e.getValue();

            Attribute attribute = attributesByName.get(attrName);
            Object inbound = instance.get(attrName);

            if (inbound == null) {
                if (attribute.isNullable()) {
                    mindlessClone.put(attrName, null);
                    continue;
                }

                throw new ValidationException("attribute must not be null: "
                        + attrName);
            }

            try {
                Object transformed = validator.validateTransform(inbound);

                mindlessClone.put(attrName, transformed);
            } catch (ClassCastException ex) {
                throw new ValidationException("invalid attribute value for '"
                        + attrName + "'");
            }
        }

        Map<String, Object> mindlessCloneInOrder = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, Object> e : instance.entrySet()) {
            String attrName = e.getKey();

            if (mindlessClone.containsKey(attrName)) {
                mindlessCloneInOrder.put(attrName, mindlessClone.get(attrName));
            } else {
                mindlessCloneInOrder.put(attrName, instance.get(attrName));
            }
        }

        return mindlessCloneInOrder;
    }

    @Override
    public Map<String, Object> untransform(Map<String, Object> instance)
            throws ValidationException {
        if (instance == null) {
            throw new ValidationException("instance must not be null");
        }

        Map<String, Object> mindlessClone = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, Object> e : instance.entrySet()) {
            String attrName = e.getKey();

            Attribute attribute = attributesByName.get(attrName);

            if (attribute == null) {
                mindlessClone.put(attrName, e.getValue());
                continue;
            }

            Object inbound = e.getValue();

            if (inbound == null) {
                if (attribute.isNullable()) {
                    mindlessClone.put(attrName, null);
                    continue;
                }

                throw new ValidationException("attribute must not be null: "
                        + attrName);
            }

            ValidatorTransformer validator = attributeValidators.get(attrName);
            Object untransformed = validator.untransform(inbound);

            mindlessClone.put(attrName, untransformed);
        }

        return mindlessClone;
    }

    public Object transformValue(String attrName, Object value)
            throws ValidationException {
        try {
            ValidatorTransformer validator = attributeValidators.get(attrName);

            return validator != null ? validator.validateTransform(value)
                    : value;
        } catch (ClassCastException e) {
            throw new ValidationException("invalid attribute value for '"
                    + attrName + "'");
        }
    }
}
