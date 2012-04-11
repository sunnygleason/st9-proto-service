package com.g414.st9.proto.service.schema;

import java.util.Collections;

public class SchemaHelper {
    public static SchemaDefinition getEmptySchema() {
        SchemaDefinition newDefinition = new SchemaDefinition(
                Collections.<Attribute> emptyList(),
                Collections.<IndexDefinition> emptyList(),
                Collections.<CounterDefinition> emptyList(),
                Collections.<FulltextDefinition> emptyList());

        return newDefinition;
    }
}
