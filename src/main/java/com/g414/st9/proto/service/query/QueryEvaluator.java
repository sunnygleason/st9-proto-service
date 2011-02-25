package com.g414.st9.proto.service.query;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class QueryEvaluator {
    public boolean matches(Map<String, Object> instance, List<QueryTerm> query) {
        boolean matches = true;

        for (QueryTerm term : query) {
            String field = term.getField();
            QueryOperator operator = term.getOperator();
            QueryValue targetValue = term.getValue();

            Object instanceValue = instance.get(field);

            if (instanceValue == null
                    && !targetValue.getValueType().equals(ValueType.NULL)) {
                matches = false;
                break;
            }

            boolean satisfied = evaluate(operator, instanceValue, targetValue);

            if (!satisfied) {
                matches = false;
                break;
            }
        }

        return matches;
    }

    public boolean evaluate(QueryOperator operator, Object instanceValue,
            QueryValue targetQueryValue) {
        Object targetValue = targetQueryValue.getValue();

        int comparison = 0;

        switch (targetQueryValue.getValueType()) {
        case INTEGER:
            try {
                comparison = -((BigInteger) targetValue)
                        .compareTo(new BigInteger(instanceValue.toString()));
            } catch (Exception e) {
                return false;
            }
            break;
        case DECIMAL:
            try {
                comparison = -((BigDecimal) targetValue)
                        .compareTo(new BigDecimal(instanceValue.toString()));
            } catch (Exception e) {
                return false;
            }
            break;
        case STRING:
            comparison = instanceValue.toString().compareTo(
                    (String) targetValue);
            break;
        case BOOLEAN:
            try {
                comparison = -((Boolean) targetValue).compareTo(Boolean
                        .parseBoolean(instanceValue.toString()));
            } catch (Exception e) {
                return false;
            }
            break;
        case NULL:
            comparison = (instanceValue == null) ? 0 : 1;
            break;
        default:
            throw new UnsupportedOperationException("Compare to "
                    + targetValue.getClass().getName() + " not supported");
        }

        boolean matches = false;

        switch (operator) {
        case EQ:
            matches = (comparison == 0);
            break;
        case NE:
            matches = (comparison != 0);
            break;
        case GE:
            matches = (comparison >= 0);
            break;
        case GT:
            matches = (comparison > 0);
            break;
        case LE:
            matches = (comparison <= 0);
            break;
        case LT:
            matches = (comparison < 0);
            break;
        default:
            throw new UnsupportedOperationException("Operator " + operator
                    + " not supported");
        }

        return matches;
    }
}
