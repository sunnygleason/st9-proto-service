package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Object class for an index definition, including name and column definitions.
 */
public class IndexDefinition {
    private final String name;
    private final List<IndexAttribute> indexColumns;
    private final Set<String> attributeNames;

    @JsonCreator
    public IndexDefinition(@JsonProperty("name") String name,
            @JsonProperty("cols") List<IndexAttribute> cols) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (cols == null || cols.isEmpty()) {
            throw new IllegalArgumentException("'cols' must not be empty");
        }

        this.name = name;

        Set<String> newAttributeNames = new HashSet<String>();
        for (IndexAttribute attr : cols) {
            newAttributeNames.add(attr.getName());
        }

        this.indexColumns = Collections.unmodifiableList(cols);
        this.attributeNames = Collections.unmodifiableSet(newAttributeNames);
    }

    public String getName() {
        return name;
    }

    @JsonProperty("cols")
    public List<IndexAttribute> getIndexAttributes() {
        return indexColumns;
    }

    @JsonIgnore
    public Set<String> getAttributeNames() {
        return attributeNames;
    }
}
