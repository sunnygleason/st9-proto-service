package com.g414.st9.proto.service.schema;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for counter column specifications (for user-defined counters).
 */
public class CounterAttribute {
    public final String name;
    public final SortOrder sortOrder;
    public final AttributeTransform transform;

    @JsonCreator
    public CounterAttribute(@JsonProperty("name") String name,
            @JsonProperty("transform") AttributeTransform transform,
            @JsonProperty("sort") SortOrder sortOrder) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (sortOrder == null) {
            throw new IllegalArgumentException("'sort' must not be null");
        }

        this.name = name;
        this.sortOrder = sortOrder;
        this.transform = (transform == null) ? AttributeTransform.NONE
                : transform;
    }

    public String getName() {
        return name;
    }

    public AttributeTransform getTransform() {
        return transform;
    }

    @JsonProperty("sort")
    public SortOrder getSortOrder() {
        return sortOrder;
    }
}
