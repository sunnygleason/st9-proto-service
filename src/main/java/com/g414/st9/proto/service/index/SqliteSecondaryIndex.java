package com.g414.st9.proto.service.index;

import com.g414.st9.proto.service.schema.AttributeType;

public class SqliteSecondaryIndex extends JDBISecondaryIndex {
    public String getSqlType(AttributeType type) {
        switch (type) {
        case BOOLEAN:
        case ENUM:
        case I8:
        case I16:
        case I32:
        case I64:
        case U8:
        case U16:
        case U32:
        case U64:
        case UTC_DATE_SECS:
            return "INT";
        case REFERENCE:
        case UTF8_SMALLSTRING:
            return "TEXT";
        default:
            throw new IllegalArgumentException("Unsupported type in index: "
                    + type);
        }
    }
}
