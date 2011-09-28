package com.g414.st9.proto.service.helper;

import com.g414.st9.proto.service.schema.AttributeType;

public class SqliteTypeHelper implements SqlTypeHelper {
    public static final String DATABASE_PREFIX = "sqlite:sqlite_";

    @Override
    public String getPrefix() {
        return DATABASE_PREFIX;
    }

    @Override
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
        case CHAR_ONE:
            return "CHAR(1)";
        default:
            throw new IllegalArgumentException("Unsupported type in index: "
                    + type);
        }
    }

    @Override
    public String getInsertIgnore() {
        return "insert or ignore";
    }

    @Override
    public String getPKConflictResolve() {
        return "on conflict replace";
    }

    @Override
    public String quote(String name) {
        return "`" + name + "`";
    }
}
