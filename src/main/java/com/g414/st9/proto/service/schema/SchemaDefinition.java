package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Schema definition class - the main entry point for schema creation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaDefinition {
    private final List<Attribute> attributes;
    private final List<IndexDefinition> indexes;

    @JsonCreator
    public SchemaDefinition(
            @JsonProperty("attributes") List<Attribute> attributes,
            @JsonProperty("indexes") List<IndexDefinition> indexes) {
        if (attributes == null || attributes.isEmpty()) {
            throw new IllegalArgumentException("'attributes' must not be empty");
        }

        if (indexes == null) {
            throw new IllegalArgumentException("'indexes' must be present");
        }

        this.attributes = Collections.unmodifiableList(attributes);
        this.indexes = Collections.unmodifiableList(indexes);
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public List<IndexDefinition> getIndexes() {
        return indexes;
    }
}
