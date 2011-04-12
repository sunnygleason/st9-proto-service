package com.g414.st9.proto.service.validator;

import com.g414.st9.proto.service.store.Key;

/**
 * Validates / transforms reference values.
 */
public class ReferenceValidator implements ValidatorTransformer<String, String> {
    private final String attribute;

    public ReferenceValidator(String attribute) {
        this.attribute = attribute;
    }

    public String validateTransform(String instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        String instanceString = instance.toString();

        try {
            return Key.valueOf(instanceString).getEncryptedIdentifier();
        } catch (Exception e) {
            throw new ValidationException("'" + attribute
                    + "' is not a valid key");
        }
    }

    public String untransform(String instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return instance;
    }
}
