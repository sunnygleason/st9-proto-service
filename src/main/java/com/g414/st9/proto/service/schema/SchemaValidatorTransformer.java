package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.g414.st9.proto.service.store.Key;
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

        Map<String, Attribute> newAttributesByName = new HashMap<String, Attribute>();

        for (Attribute attribute : schemaDefinition.getAttributes()) {
            newAttributesByName.put(attribute.getName(), attribute);
        }

        this.attributesByName = Collections
                .unmodifiableMap(newAttributesByName);

        Map<String, ValidatorTransformer<?, ?>> newAttributeValidators = new HashMap<String, ValidatorTransformer<?, ?>>();

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

        for (Map.Entry<String, Object> e : instance.entrySet()) {
            String attrName = e.getKey();

            Attribute attribute = attributesByName.get(attrName);

            if (attribute == null) {
                mindlessClone.put(attrName, e.getValue());
                continue;
            }

            Object inbound = e.getValue();

            if (inbound == null) {
                mindlessClone.put(attrName, null);
                continue;
            }

            ValidatorTransformer validator = attributeValidators.get(attrName);

            Object transformed = validator.validateTransform(inbound);

            if (AttributeType.REFERENCE.equals(attribute.getType())) {
                Key key = (Key) transformed;
                mindlessClone.put(attribute.getName() + "$t", key.getType());
                mindlessClone.put(attribute.getName() + "$i", key.getId());
            } else {
                mindlessClone.put(attribute.getName(), transformed);
            }
        }

        return mindlessClone;
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

            if (attrName.endsWith("$i")) {
                continue;
            }

            if (attrName.endsWith("$t")) {
                attrName = attrName.substring(0, attrName.length() - 2);
            }

            Attribute attribute = attributesByName.get(attrName);

            if (attribute == null) {
                mindlessClone.put(attrName, e.getValue());
                continue;
            }

            Object inbound = null;

            if (AttributeType.REFERENCE.equals(attribute.getType())) {
                inbound = Key.valueOf(instance.get(attrName + "$t") + ":"
                        + instance.get(attrName + "$i"));
            } else {
                inbound = e.getValue();
            }

            ValidatorTransformer validator = attributeValidators.get(attrName);
            Object untransformed = validator.untransform(inbound);

            mindlessClone.put(attrName, untransformed);
        }

        return mindlessClone;
    }
}
