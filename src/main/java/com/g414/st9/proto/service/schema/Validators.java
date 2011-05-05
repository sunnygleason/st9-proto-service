package com.g414.st9.proto.service.schema;

import com.g414.st9.proto.service.validator.AnyValidator;
import com.g414.st9.proto.service.validator.BooleanValidator;
import com.g414.st9.proto.service.validator.EnumValidator;
import com.g414.st9.proto.service.validator.IntegerValidator;
import com.g414.st9.proto.service.validator.ReferenceValidator;
import com.g414.st9.proto.service.validator.StringValidator;
import com.g414.st9.proto.service.validator.UTCDateValidator;
import com.g414.st9.proto.service.validator.ValidatorTransformer;

/**
 * Helper class for mapping attribute types to ValidatorTransformer instances.
 */
public class Validators {
    public ValidatorTransformer<?, ?> validatorFor(Attribute attribute) {
        switch (attribute.getType()) {
        case ANY:
            return new AnyValidator(attribute.getName());
        case BOOLEAN:
            return new BooleanValidator(attribute.getName());
        case ENUM:
            return new EnumValidator(attribute.getName(), attribute.getValues());
        case U8:
            return new IntegerValidator(attribute.getName(), "0",
                    Integer.toString(0x000000FF));
        case U16:
            return new IntegerValidator(attribute.getName(), "0",
                    Integer.toString(0x0000FFFF));
        case U32:
            return new IntegerValidator(attribute.getName(), "0",
                    Long.toString(0x00000000FFFFFFFFL));
        case U64:
            return new IntegerValidator(attribute.getName(), "0",
                    "18446744073709551615");

        case I8:
            return new IntegerValidator(attribute.getName(),
                    Integer.toString(Byte.MIN_VALUE),
                    Integer.toString(Byte.MAX_VALUE));
        case I16:
            return new IntegerValidator(attribute.getName(),
                    Integer.toString(Character.MIN_VALUE),
                    Integer.toString(Character.MAX_VALUE));
        case I32:
            return new IntegerValidator(attribute.getName(),
                    Integer.toString(Integer.MIN_VALUE),
                    Integer.toString(Integer.MAX_VALUE));
        case I64:
            return new IntegerValidator(attribute.getName(),
                    Long.toString(Long.MIN_VALUE),
                    Long.toString(Long.MAX_VALUE));

        case REFERENCE:
            return new ReferenceValidator(attribute.getName());
        case UTC_DATE_SECS:
            return new UTCDateValidator(attribute.getName());
        case UTF8_SMALLSTRING:
        case UTF8_TEXT:
            return new StringValidator(attribute.getName(),
                    attribute.getMinlength(), attribute.getMaxlength());
        default:
            throw new IllegalArgumentException("Unknown type "
                    + attribute.getType());
        }
    }
}
