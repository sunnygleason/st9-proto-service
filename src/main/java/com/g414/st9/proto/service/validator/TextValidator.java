package com.g414.st9.proto.service.validator;

/**
 * Validates / transforms String values.
 */
public class TextValidator implements ValidatorTransformer<Object, String> {
    private final String attribute;
    private final Integer minLength;
    private final Integer maxLength;

    public TextValidator(String attribute, Integer minLength,
            Integer maxLength) {
        this.attribute = attribute;
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    public String validateTransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        String instanceString = instance.toString();
        int length = instanceString.length();

        if (minLength != null && length < minLength) {
            throw new ValidationException("'" + attribute
                    + "' length be greater than or equal to " + minLength);
        }

        if (maxLength != null && length > maxLength) {
            throw new ValidationException("'" + attribute
                    + "' length be less than or equal to " + minLength);
        }

        return instanceString;
    }

    public Object untransform(String instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return instance;
    }
}
