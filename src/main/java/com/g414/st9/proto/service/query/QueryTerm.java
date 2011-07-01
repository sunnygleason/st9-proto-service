package com.g414.st9.proto.service.query;

/**
 * A query term, representing one boolean clause in a conjunction of possibly
 * several clauses.
 */
public class QueryTerm {
    private final QueryOperator operator;
    private final String field;
    private final QueryValue value;
    private final QueryValueList valueList;

    public QueryTerm(QueryOperator operator, String field, QueryValue value) {
        this.operator = operator;
        this.field = field;
        this.value = value;
        this.valueList = null;
    }

    public QueryTerm(QueryOperator operator, String field,
            QueryValueList valueList) {
        this.operator = operator;
        this.field = field;
        this.value = null;
        this.valueList = valueList;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public String getField() {
        return field;
    }

    public QueryValue getValue() {
        if (valueList != null) {
            throw new IllegalStateException(
                    "value() not present in this term, use valueList() instead");
        }

        return value;
    }

    public QueryValueList getValueList() {
        if (value != null) {
            throw new IllegalStateException(
                    "valueList() not present in this term, use value() instead");
        }

        return valueList;
    }

    @Override
    public String toString() {
        return this.field + " " + this.operator.name() + " " + this.value;
    }
}
