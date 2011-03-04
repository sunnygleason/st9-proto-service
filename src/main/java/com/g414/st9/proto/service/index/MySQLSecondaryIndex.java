package com.g414.st9.proto.service.index;

import com.g414.st9.proto.service.schema.AttributeType;

public class MySQLSecondaryIndex extends JDBISecondaryIndex {
    public String getSqlType(AttributeType type) {
        switch (type) {
        case BOOLEAN:
            return "TINYINT";
        case ENUM:
            return "SMALLINT";
        case I8:
            return "TINYINT";
        case I16:
            return "SMALLINT";
        case I32:
            return "INT";
        case I64:
            return "BIGINT";
        case U8:
            return "TINYINT UNSIGNED";
        case U16:
            return "SMALLINT UNSIGNED";
        case U32:
            return "INT UNSIGNED";
        case U64:
            return "BIGINT UNSIGNED";
        case UTC_DATE_SECS:
            return "INT UNSIGNED";
        case REFERENCE:
        case UTF8_SMALLSTRING:
            return "VARCHAR(255)";
        default:
            throw new IllegalArgumentException("Unsupported type in index: "
                    + type);
        }
    }
}
