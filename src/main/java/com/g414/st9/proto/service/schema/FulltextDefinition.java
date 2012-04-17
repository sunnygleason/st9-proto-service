package com.g414.st9.proto.service.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Object class for a counter definition, including name and column definitions.
 */
public class FulltextDefinition {
    private final String name;
    private final String parentType;
    private final String parentIdentifierAttribute;
    private final List<FulltextAttribute> counterAttributes;
    private final List<String> attributeNames;

    @JsonCreator
    public FulltextDefinition(
            @JsonProperty("name") String name,
            @JsonProperty("parentType") String parentType,
            @JsonProperty("parentIdentifierAttribute") String parentIdentifierAttribute,
            @JsonProperty("cols") List<FulltextAttribute> cols) {
        if (name == null) {
            throw new IllegalArgumentException("'name' must not be null");
        }

        if (cols == null || cols.isEmpty()) {
            throw new IllegalArgumentException("'cols' must not be empty");
        }

        for (FulltextAttribute col : cols) {
            if ("id".equals(col.getName()) || "type".equals(col.getName())) {
                throw new IllegalArgumentException(
                        "'cols' must not contain 'id' or 'type' fields");
            }
        }

        this.name = name;
        this.parentType = parentType;
        this.parentIdentifierAttribute = parentIdentifierAttribute;

        List<String> newAttributeNames = new ArrayList<String>();
        for (FulltextAttribute attr : cols) {
            newAttributeNames.add(attr.getName());
        }

        this.counterAttributes = Collections.unmodifiableList(cols);
        this.attributeNames = Collections.unmodifiableList(newAttributeNames);
    }

    public String getName() {
        return name;
    }

    public String getParentType() {
        return parentType;
    }

    public String getParentIdentifierAttribute() {
        return parentIdentifierAttribute;
    }

    @JsonProperty("cols")
    public List<FulltextAttribute> getFullTextAttributes() {
        return counterAttributes;
    }

    @JsonIgnore
    public List<String> getAttributeNames() {
        return attributeNames;
    }
}
