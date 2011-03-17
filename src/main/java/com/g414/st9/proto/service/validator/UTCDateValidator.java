package com.g414.st9.proto.service.validator;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * Validates / transforms UTC date values.
 */
public class UTCDateValidator implements ValidatorTransformer<Object, Number> {
    private final DateTimeFormatter format = ISODateTimeFormat
            .basicDateTimeNoMillis();
    private final String attribute;

    public UTCDateValidator(String attribute) {
        this.attribute = attribute;
    }

    public Long validateTransform(Object instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        try {
            String instanceString = (instance instanceof DateTime) ? format
                    .print((DateTime) instance) : instance.toString();

            return format.parseMillis(instanceString) / 1000L;
        } catch (IllegalArgumentException e) {
            throw new ValidationException("'" + attribute
                    + "' is not ISO8601 datetime (yyyyMMdd'T'HHmmssZ)");
        }
    }

    @Override
    public Object untransform(Number instance) throws ValidationException {
        if (instance == null) {
            throw new ValidationException("'" + attribute
                    + "' must not be null");
        }

        return format.print(new DateTime(instance.longValue() * 1000L,
                DateTimeZone.UTC));
    }
}
