package com.g414.st9.proto.service.schema;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for index column specifications (for user-defined secondary indexes).
 */
public class IndexAttribute {
    public final String name;
    public final SortOrder sortOrder;

    @JsonCreator
    public IndexAttribute(@JsonProperty("name") String name,
            @JsonProperty("sort") SortOrder sortOrder) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (sortOrder == null) {
            throw new IllegalArgumentException("'sort' must not be null");
        }

        this.name = name;
        this.sortOrder = sortOrder;
    }

    public String getName() {
        return name;
    }

    @JsonProperty("sort")
    public SortOrder getSortOrder() {
        return sortOrder;
    }
}
