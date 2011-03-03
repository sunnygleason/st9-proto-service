package com.g414.st9.proto.service.validator;

/**
 * Validates / transforms Boolean values.
 */
public class BooleanValidator implements ValidatorTransformer<Object, Boolean> {
    private final String attribute;

    public BooleanValidator(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Boolean validateTransform(Object instance)
            throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        String instanceString = instance.toString();

        return Boolean.valueOf(instanceString);
    }

    @Override
    public Object untransform(Boolean instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return instance;
    }
}
