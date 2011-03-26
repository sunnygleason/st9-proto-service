package com.g414.st9.proto.service.query;

/**
 * A query term, representing one boolean clause in a conjunction of possibly
 * several clauses.
 */
public class QueryTerm {
    private final QueryOperator operator;
    private final String field;
    private final QueryValue value;

    public QueryTerm(QueryOperator operator, String field, QueryValue value) {
        this.operator = operator;
        this.field = field;
        this.value = value;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public String getField() {
        return field;
    }

    public QueryValue getValue() {
        return value;
    }

    @Override
    public String toString() {
        return this.field + " " + this.operator.name() + " " + this.value;
    }
}
