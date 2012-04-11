package com.g414.st9.proto.service.schema;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class for full-text index column specifications.
 */
public class FulltextAttribute {
    public final String name;
    public final AttributeTransform transform;

    @JsonCreator
    public FulltextAttribute(@JsonProperty("name") String name,
            @JsonProperty("transform") AttributeTransform transform) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        this.name = name;
        this.transform = (transform == null) ? AttributeTransform.NONE
                : transform;
    }

    public String getName() {
        return name;
    }

    public AttributeTransform getTransform() {
        return transform;
    }
}
