package com.g414.st9.proto.service.validator;

import java.util.List;

/**
 * Validates "ARRAY" type
 */
public class ArrayValidator implements ValidatorTransformer<Object, Object> {
    private final String attribute;

    public ArrayValidator(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Object validateTransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        if (!(instance instanceof List)) {
            throw new ValidationException("'" + attribute
                    + "' must be an array");
        }

        return instance;
    }

    @Override
    public Object untransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        if (!(instance instanceof List)) {
            throw new ValidationException("'" + attribute
                    + "' must be an array");
        }

        return instance;
    }
}
