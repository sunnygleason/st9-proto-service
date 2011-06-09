package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Object class for a counter definition, including name and column definitions.
 */
public class CounterDefinition {
    private final String name;
    private final List<CounterAttribute> counterAttributes;
    private final Set<String> attributeNames;

    @JsonCreator
    public CounterDefinition(@JsonProperty("name") String name,
            @JsonProperty("cols") List<CounterAttribute> cols) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (cols == null || cols.isEmpty()) {
            throw new IllegalArgumentException("'cols' must not be empty");
        }

        for (CounterAttribute col : cols) {
            if ("id".equals(col.getName()) || "type".equals(col.getName())) {
                throw new IllegalArgumentException(
                        "'cols' must not contain 'id' or 'type' fields");
            }
        }

        this.name = name;

        Set<String> newAttributeNames = new HashSet<String>();
        for (CounterAttribute attr : cols) {
            newAttributeNames.add(attr.getName());
        }

        this.counterAttributes = Collections.unmodifiableList(cols);
        this.attributeNames = Collections.unmodifiableSet(newAttributeNames);
    }

    public String getName() {
        return name;
    }

    @JsonProperty("cols")
    public List<CounterAttribute> getCounterAttributes() {
        return counterAttributes;
    }

    @JsonIgnore
    public Set<String> getAttributeNames() {
        return attributeNames;
    }
}
