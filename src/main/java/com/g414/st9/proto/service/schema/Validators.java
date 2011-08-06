package com.g414.st9.proto.service.schema;

import com.g414.st9.proto.service.validator.AnyValidator;
import com.g414.st9.proto.service.validator.BooleanValidator;
import com.g414.st9.proto.service.validator.EnumValidator;
import com.g414.st9.proto.service.validator.IntegerValidator;
import com.g414.st9.proto.service.validator.ArrayValidator;
import com.g414.st9.proto.service.validator.MapValidator;
import com.g414.st9.proto.service.validator.ReferenceValidator;
import com.g414.st9.proto.service.validator.SmallStringValidator;
import com.g414.st9.proto.service.validator.TextValidator;
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
                    Long.toString(Long.MAX_VALUE)
            /* was "18446744073709551615", but that's unsupported by h2 */);

        case I8:
            return new IntegerValidator(attribute.getName(),
                    Integer.toString(Byte.MIN_VALUE),
                    Integer.toString(Byte.MAX_VALUE));
        case I16:
            return new IntegerValidator(attribute.getName(),
                    Integer.toString(-32768), Integer.toString(32767));
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
            return new SmallStringValidator(attribute.getName(),
                    attribute.getMinlength(), attribute.getMaxlength());
        case UTF8_TEXT:
            return new TextValidator(attribute.getName(),
                    attribute.getMinlength(), attribute.getMaxlength());
        case ARRAY:
            return new ArrayValidator(attribute.getName());
        case MAP:
            return new MapValidator(attribute.getName());

        default:
            throw new IllegalArgumentException("Unknown type "
                    + attribute.getType());
        }
    }
}
