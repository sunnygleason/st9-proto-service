package com.g414.st9.proto.service.helper;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.g414.st9.proto.service.schema.CounterDefinition;
import com.g414.st9.proto.service.schema.FulltextDefinition;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.google.common.collect.ImmutableMap;

public class EntityDiffHelper {
    public static Map<String, Object> diffValues(
            IndexDefinition indexDefinition, Map<String, Object> prev,
            Map<String, Object> curr) {
        return getDiff(null, prev, curr, indexDefinition.getAttributeNames());
    }

    public static Map<String, Object> diffValues(
            CounterDefinition counterDefinition, Map<String, Object> prev,
            Map<String, Object> curr) {
        return getDiff(null, prev, curr, counterDefinition.getAttributeNames());
    }

    public static Map<String, Object> diffValues(
            FulltextDefinition fullTextDefinition, Map<String, Object> prev,
            Map<String, Object> curr) {
        return getDiff(fullTextDefinition.getParentIdentifierAttribute(), prev,
                curr, fullTextDefinition.getAttributeNames());
    }

    private static Map<String, Object> getDiff(
            String parentIdentifierAttribute, Map<String, Object> prev,
            Map<String, Object> curr, List<String> attributeNames) {
        Map<String, Object> prevDiffChanged = new LinkedHashMap<String, Object>();
        Map<String, Object> currDiffChanged = new LinkedHashMap<String, Object>();

        String parentIdentifier = (parentIdentifierAttribute != null) ? (String) curr
                .get(parentIdentifierAttribute) : null;
        if (parentIdentifier != null) {
            currDiffChanged.put("_parent", parentIdentifier);
        }

        boolean hasDiff = false;

        for (String name : attributeNames) {
            Object prevValue = prev.get(name);
            Object currValue = curr.get(name);

            prevDiffChanged.put(name, prevValue);
            currDiffChanged.put(name, currValue);

            if (!objectsAreEqual(prevValue, currValue)) {
                hasDiff = true;
            }
        }

        return !hasDiff ? Collections.<String, Object> emptyMap()
                : ImmutableMap.<String, Object> of("prev", prevDiffChanged,
                        "curr", currDiffChanged);
    }

    private static boolean objectsAreEqual(Object a, Object b) {
        return (a == null && b == null)
                || ((a != null && b != null) && (a.equals(b)));
    }
}
