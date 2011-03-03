package com.g414.st9.proto.service.validator;

import java.util.List;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;

/**
 * Validates / transforms values using the schema-defined ENUM.
 */
public class EnumValidator implements ValidatorTransformer<Object, Integer> {
    private final String attribute;
    private final BiMap<String, Integer> enumMapping;

    public EnumValidator(String attribute, List<String> values) {
        this.attribute = attribute;
        int id = 0;
        BiMap<String, Integer> biMap = HashBiMap.create();
        for (String value : values) {
            biMap.put(value, id++);
        }

        this.enumMapping = ImmutableBiMap.copyOf(biMap);
    }

    @Override
    public Integer validateTransform(Object instance)
            throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        String instanceString = instance.toString();

        if (!enumMapping.containsKey(instanceString)) {
            throw new ValidationException("'" + attribute
                    + "' is not a valid enum value : " + instanceString);
        }

        return enumMapping.get(instanceString);
    }

    @Override
    public Object untransform(Integer instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return enumMapping.inverse().get(instance);
    }
}
