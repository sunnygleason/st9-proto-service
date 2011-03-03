package com.g414.st9.proto.service.schema;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for index column specifications (for user-defined secondary indexes).
 */
public class IndexColumn {
    public final String attributeName;
    public final SortOrder sortOrder;

    @JsonCreator
    public IndexColumn(@JsonProperty("name") String attributeName,
            @JsonProperty("sort") SortOrder sortOrder) {
        if (attributeName == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (sortOrder == null) {
            throw new IllegalArgumentException("'sort' must not be null");
        }

        this.attributeName = attributeName;
        this.sortOrder = sortOrder;
    }

    @JsonProperty("name")
    public String getAttributeName() {
        return attributeName;
    }

    @JsonProperty("order")
    public SortOrder getSortOrder() {
        return sortOrder;
    }
}
