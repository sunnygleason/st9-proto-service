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
    private final List<CounterDefinition> counters;
    private final Map<String, Attribute> attributeMap;
    private final Map<String, IndexDefinition> indexMap;
    private final Map<String, CounterDefinition> counterMap;

    @JsonCreator
    public SchemaDefinition(
            @JsonProperty("attributes") List<Attribute> attributes,
            @JsonProperty("indexes") List<IndexDefinition> indexes,
            @JsonProperty("counters") List<CounterDefinition> counters) {
        if (attributes == null) {
            throw new IllegalArgumentException("'attributes' must be present");
        }

        if (indexes == null) {
            throw new IllegalArgumentException("'indexes' must be present");
        }

        if (counters == null) {
            throw new IllegalArgumentException("'counters' must be present");
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
        this.counters = Collections.unmodifiableList(counters);

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

        Map<String, CounterDefinition> newCounters = new LinkedHashMap<String, CounterDefinition>();
        for (CounterDefinition counter : counters) {
            newCounters.put(counter.getName(), counter);
        }

        this.counterMap = Collections.unmodifiableMap(newCounters);
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public List<IndexDefinition> getIndexes() {
        return indexes;
    }

    public List<CounterDefinition> getCounters() {
        return counters;
    }

    @JsonIgnore
    public Map<String, Attribute> getAttributesMap() {
        return attributeMap;
    }

    @JsonIgnore
    public Map<String, IndexDefinition> getIndexMap() {
        return indexMap;
    }

    @JsonIgnore
    public Map<String, CounterDefinition> getCounterMap() {
        return counterMap;
    }
}
