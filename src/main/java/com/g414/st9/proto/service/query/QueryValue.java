package com.g414.st9.proto.service.query;

import java.math.BigDecimal;
import java.math.BigInteger;

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
