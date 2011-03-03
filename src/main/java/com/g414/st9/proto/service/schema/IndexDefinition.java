package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Object class for an index definition, including name and column definitions.
 */
public class IndexDefinition {
    private final String name;
    private List<IndexColumn> indexColumns;

    @JsonCreator
    public IndexDefinition(@JsonProperty("name") String name,
            @JsonProperty("indexColumns") List<IndexColumn> indexColumns) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (indexColumns == null || indexColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "'indexColumns' must not be empty");
        }

        this.name = name;
        this.indexColumns = Collections.unmodifiableList(indexColumns);
    }

    public String getName() {
        return name;
    }

    public List<IndexColumn> getIndexColumns() {
        return indexColumns;
    }
}
