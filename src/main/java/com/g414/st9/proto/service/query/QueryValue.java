package com.g414.st9.proto.service.query;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A modestly intelligent holder class for the value in the query; Object just
 * isn't rich enough, so we use this class to hold a "type" enum telling us how
 * to use the value provided by the holder. If you told me to rename this class
 * to ValueHolder, I might agree with you.
 */
public class QueryValue {
    private final ValueType valueType;
    private final String literal;
    private final Object value;

    public QueryValue(ValueType valueType, String literal) {
        this.valueType = valueType;
        this.literal = literal;
        switch (valueType) {
        case DECIMAL:
            this.value = new BigDecimal(literal);
            break;
        case INTEGER:
            this.value = new BigInteger(literal);
            break;
        case STRING:
            this.value = literal.substring(1, literal.length() - 1);
            break;
        case BOOLEAN:
            this.value = Boolean.valueOf(literal);
            break;
        case NULL:
            this.value = "";
            break;
        default:
            throw new IllegalArgumentException("unknown type: " + valueType);
        }
    }

    public ValueType getValueType() {
        return valueType;
    }

    public String getLiteral() {
        return literal;
    }

    public Object getValue() {
        return value;
    }
}
