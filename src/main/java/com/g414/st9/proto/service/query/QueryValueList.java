package com.g414.st9.proto.service.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.g414.st9.proto.service.validator.ValidationException;

/**
 * Represents a list of values, typically for an IN clause.
 */
public class QueryValueList {
    private final List<QueryValue> valueList;

    public QueryValueList(List<QueryValue> values) {
        if (values == null) {
            throw new ValidationException("parameter values may not be null");
        }

        if (values.size() < 1) {
            throw new ValidationException(
                    "parameter values must have at least one value");
        }

        if (values.size() > 100) {
            throw new ValidationException(
                    "parameter values must have 100 or fewer values");
        }

        List<QueryValue> newValueList = new ArrayList<QueryValue>();
        newValueList.addAll(values);
        this.valueList = Collections.unmodifiableList(newValueList);
    }

    public List<QueryValue> getValueList() {
        return valueList;
    }

    @Override
    public String toString() {
        return valueList.toString();
    }
}
