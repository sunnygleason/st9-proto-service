package utest.com.g414.st9.proto.service.query;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.query.QueryEvaluator;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;

@Test
public class QueryEvaluatorTest {
    private QueryEvaluator e = new QueryEvaluator();

    public void testIntegerComparisons() {
        assertAll(true, "1", i("1"), QueryOperator.EQ, QueryOperator.GE,
                QueryOperator.LE);
        assertAll(false, "1", i("1"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.LT);

        assertAll(true, Long.valueOf(1), i("1"), QueryOperator.EQ,
                QueryOperator.GE, QueryOperator.LE);
        assertAll(false, Long.valueOf(1), i("1"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.LT);

        assertAll(true, "2", i("1"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.GE);
        assertAll(false, "2", i("1"), QueryOperator.EQ, QueryOperator.LT,
                QueryOperator.LE);

        assertAll(true, Long.valueOf(2), i("1"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.GE);
        assertAll(false, Long.valueOf(2), i("1"), QueryOperator.EQ,
                QueryOperator.LT, QueryOperator.LE);

        assertAll(true, "1", i("2"), QueryOperator.NE, QueryOperator.LT,
                QueryOperator.LE);
        assertAll(false, "1", i("2"), QueryOperator.EQ, QueryOperator.GT,
                QueryOperator.GE);

        assertAll(true, Long.valueOf(1), i("2"), QueryOperator.NE,
                QueryOperator.LT, QueryOperator.LE);
        assertAll(false, Long.valueOf(1), i("2"), QueryOperator.EQ,
                QueryOperator.GT, QueryOperator.GE);
    }

    public void testDecimalComparisons() {
        assertAll(true, "1.0", d("1.0"), QueryOperator.EQ, QueryOperator.GE,
                QueryOperator.LE);
        assertAll(false, "1.0", d("1.0"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.LT);

        assertAll(true, Double.valueOf(1.0), d("1.0"), QueryOperator.EQ,
                QueryOperator.GE, QueryOperator.LE);
        assertAll(false, Double.valueOf(1.0), d("1.0"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.LT);

        assertAll(true, "2.0", d("1.0"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.GE);
        assertAll(false, "2.0", d("1.0"), QueryOperator.EQ, QueryOperator.LT,
                QueryOperator.LE);

        assertAll(true, Double.valueOf(2.0), d("1.0"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.GE);
        assertAll(false, Double.valueOf(2.0), d("1.0"), QueryOperator.EQ,
                QueryOperator.LT, QueryOperator.LE);

        assertAll(true, "1.0", d("2.0"), QueryOperator.NE, QueryOperator.LT,
                QueryOperator.LE);
        assertAll(false, "1.0", d("2.0"), QueryOperator.EQ, QueryOperator.GT,
                QueryOperator.GE);

        assertAll(true, Double.valueOf(1.0), d("2.0"), QueryOperator.NE,
                QueryOperator.LT, QueryOperator.LE);
        assertAll(false, Double.valueOf(1.0), d("2.0"), QueryOperator.EQ,
                QueryOperator.GT, QueryOperator.GE);
    }

    public void testStringComparisons() {
        assertAll(true, "", s(""), QueryOperator.EQ, QueryOperator.GE,
                QueryOperator.LE);
        assertAll(false, "", s(""), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.LT);

        assertAll(true, "aaa", s("aaa"), QueryOperator.EQ, QueryOperator.GE,
                QueryOperator.LE);
        assertAll(false, "aaa", s("aaa"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.LT);

        assertAll(true, "bbb", s("aaa"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.GE);
        assertAll(false, "bbb", s("aaa"), QueryOperator.EQ, QueryOperator.LT,
                QueryOperator.LE);

        assertAll(true, "b", s("aaa"), QueryOperator.NE, QueryOperator.GT,
                QueryOperator.GE);
        assertAll(false, "b", s("aaa"), QueryOperator.EQ, QueryOperator.LT,
                QueryOperator.LE);

        assertAll(true, "aaa", s("bbb"), QueryOperator.NE, QueryOperator.LT,
                QueryOperator.LE);
        assertAll(false, "aaa", s("bbb"), QueryOperator.EQ, QueryOperator.GT,
                QueryOperator.GE);

        assertAll(true, "a", s("bbb"), QueryOperator.NE, QueryOperator.LT,
                QueryOperator.LE);
        assertAll(false, "a", s("bbb"), QueryOperator.EQ, QueryOperator.GT,
                QueryOperator.GE);
    }

    private void assertAll(boolean shouldBe, Object instance,
            QueryValue target, QueryOperator... operators) {
        for (QueryOperator operator : operators) {
            Assert.assertEquals(e.evaluate(operator, instance, target),
                    shouldBe);
        }
    }

    private static QueryValue s(String value) {
        return new QueryValue(ValueType.STRING, "\"" + value + "\"");
    }

    private static QueryValue i(String value) {
        return new QueryValue(ValueType.INTEGER, value);
    }

    private static QueryValue d(String value) {
        return new QueryValue(ValueType.DECIMAL, value);
    }
}
