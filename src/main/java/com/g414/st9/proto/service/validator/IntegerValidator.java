package com.g414.st9.proto.service.validator;

import java.math.BigInteger;

/**
 * Validates / transforms integer values.
 */
public class IntegerValidator implements ValidatorTransformer<Object, Number> {
    private final String attribute;
    private final BigInteger min;
    private final BigInteger max;

    public IntegerValidator(String attribute, String minString, String maxString) {
        this.attribute = attribute;
        this.min = new BigInteger(minString);
        this.max = new BigInteger(maxString);
    }

    public BigInteger validateTransform(Object instance)
            throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        try {
            String instanceString = instance.toString();
            BigInteger instanceInteger = new BigInteger(instanceString);

            if (instanceInteger.compareTo(min) < 0) {
                throw new ValidationException("'" + attribute
                        + "' must be greater than or equal to " + min);
            } else if (instanceInteger.compareTo(max) > 0) {
                throw new ValidationException("'" + attribute
                        + "' must be less than or equal to " + max);
            }

            return instanceInteger;
        } catch (NumberFormatException e) {
            throw new ValidationException("'" + attribute
                    + "' is not a valid integer");
        }
    }

    @Override
    public Object untransform(Number instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        BigInteger bigInstance = (instance instanceof BigInteger) ? (BigInteger) instance
                : new BigInteger(instance.toString());
        Object result = instance;

        int compareMin = bigInstance.compareTo(BigInteger
                .valueOf(Long.MIN_VALUE));
        int compareMax = bigInstance.compareTo(BigInteger
                .valueOf(Long.MAX_VALUE));
        if (compareMin >= 0 && compareMax <= 0) {
            result = instance.longValue();
        }

        return result;
    }
}
