package com.g414.st9.proto.service.query;

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
}
