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
        if (attributes == null) {
            throw new IllegalArgumentException("'attributes' must be present");
        }

        if (indexes == null) {
            throw new IllegalArgumentException("'indexes' must be present");
        }

        boolean isFirst = true;
        if (!indexes.isEmpty()) {
            for (IndexDefinition def : indexes) {
                if (def.isUnique() && !isFirst) {
                    throw new IllegalArgumentException(
                            "at most one unique 'index' may be present per schema and must be the first index listed in order");
                }

                isFirst = false;
            }
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
