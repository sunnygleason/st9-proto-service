package com.g414.st9.proto.service.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Schema definition class - the main entry point for schema creation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaDefinition {
    private final List<Attribute> attributes;
    private final List<IndexDefinition> indexes;
    private final Map<String, Attribute> attributeMap;
    private final Map<String, IndexDefinition> indexMap;

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

        Map<String, Attribute> newAttributes = new LinkedHashMap<String, Attribute>();
        for (Attribute attr : attributes) {
            newAttributes.put(attr.getName(), attr);
        }

        this.attributeMap = Collections.unmodifiableMap(newAttributes);

        Map<String, IndexDefinition> newIndexes = new LinkedHashMap<String, IndexDefinition>();
        for (IndexDefinition index : indexes) {
            newIndexes.put(index.getName(), index);
        }

        this.indexMap = Collections.unmodifiableMap(newIndexes);
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public List<IndexDefinition> getIndexes() {
        return indexes;
    }

    @JsonIgnore
    public Map<String, Attribute> getAttributesMap() {
        return attributeMap;
    }

    @JsonIgnore
    public Map<String, IndexDefinition> getIndexMap() {
        return indexMap;
    }
}
