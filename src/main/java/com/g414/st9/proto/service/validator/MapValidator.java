package com.g414.st9.proto.service.validator;

import java.util.Map;

/**
 * Validates "ARRAY" type
 */
public class MapValidator implements ValidatorTransformer<Object, Object> {
    private final String attribute;

    public MapValidator(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Object validateTransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        if (!(instance instanceof Map)) {
            throw new ValidationException("'" + attribute + "' must be a map");
        }

        return instance;
    }

    @Override
    public Object untransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        if (!(instance instanceof Map)) {
            throw new ValidationException("'" + attribute + "' must be a map");
        }

        return instance;
    }
}
