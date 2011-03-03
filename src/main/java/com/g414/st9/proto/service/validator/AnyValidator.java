package com.g414.st9.proto.service.validator;

/**
 * Validates "ANY" type - is simply the identity transform.
 */
public class AnyValidator implements ValidatorTransformer<Object, Object> {
    private final String attribute;

    public AnyValidator(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public Object validateTransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return instance;
    }

    @Override
    public Object untransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return instance;
    }
}
