package utest.com.g414.st9.proto.service.query;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.query.QueryEvaluator;
import com.g414.st9.proto.service.query.QueryOperator;

@Test
public class QueryEvaluatorTest {
    private QueryEvaluator e = new QueryEvaluator();

    public void testIntegerComparisons() {
        assertAll(true, "1", new BigInteger("1"), QueryOperator.EQ,
                QueryOperator.GE, QueryOperator.LE);
        assertAll(false, "1", new BigInteger("1"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.LT);

        assertAll(true, Long.valueOf(1), new BigInteger("1"), QueryOperator.EQ,
                QueryOperator.GE, QueryOperator.LE);
        assertAll(false, Long.valueOf(1), new BigInteger("1"),
                QueryOperator.NE, QueryOperator.GT, QueryOperator.LT);

        assertAll(true, "2", new BigInteger("1"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.GE);
        assertAll(false, "2", new BigInteger("1"), QueryOperator.EQ,
                QueryOperator.LT, QueryOperator.LE);

        assertAll(true, Long.valueOf(2), new BigInteger("1"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.GE);
        assertAll(false, Long.valueOf(2), new BigInteger("1"),
                QueryOperator.EQ, QueryOperator.LT, QueryOperator.LE);

        assertAll(true, "1", new BigInteger("2"), QueryOperator.NE,
                QueryOperator.LT, QueryOperator.LE);
        assertAll(false, "1", new BigInteger("2"), QueryOperator.EQ,
                QueryOperator.GT, QueryOperator.GE);

        assertAll(true, Long.valueOf(1), new BigInteger("2"), QueryOperator.NE,
                QueryOperator.LT, QueryOperator.LE);
        assertAll(false, Long.valueOf(1), new BigInteger("2"),
                QueryOperator.EQ, QueryOperator.GT, QueryOperator.GE);
    }

    public void testDecimalComparisons() {
        assertAll(true, "1.0", new BigDecimal("1.0"), QueryOperator.EQ,
                QueryOperator.GE, QueryOperator.LE);
        assertAll(false, "1.0", new BigDecimal("1.0"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.LT);

        assertAll(true, Double.valueOf(1.0), new BigDecimal("1.0"),
                QueryOperator.EQ, QueryOperator.GE, QueryOperator.LE);
        assertAll(false, Double.valueOf(1.0), new BigDecimal("1.0"),
                QueryOperator.NE, QueryOperator.GT, QueryOperator.LT);

        assertAll(true, "2.0", new BigDecimal("1.0"), QueryOperator.NE,
                QueryOperator.GT, QueryOperator.GE);
        assertAll(false, "2.0", new BigDecimal("1.0"), QueryOperator.EQ,
                QueryOperator.LT, QueryOperator.LE);

        assertAll(true, Double.valueOf(2.0), new BigDecimal("1.0"),
                QueryOperator.NE, QueryOperator.GT, QueryOperator.GE);
        assertAll(false, Double.valueOf(2.0), new BigDecimal("1.0"),
                QueryOperator.EQ, QueryOperator.LT, QueryOperator.LE);

        assertAll(true, "1.0", new BigDecimal("2.0"), QueryOperator.NE,
                QueryOperator.LT, QueryOperator.LE);
        assertAll(false, "1.0", new BigDecimal("2.0"), QueryOperator.EQ,
                QueryOperator.GT, QueryOperator.GE);

        assertAll(true, Double.valueOf(1.0), new BigDecimal("2.0"),
                QueryOperator.NE, QueryOperator.LT, QueryOperator.LE);
        assertAll(false, Double.valueOf(1.0), new BigDecimal("2.0"),
                QueryOperator.EQ, QueryOperator.GT, QueryOperator.GE);
    }

    public void testStringComparisons() {
        assertAll(true, "", "", QueryOperator.EQ, QueryOperator.GE,
                QueryOperator.LE);
        assertAll(false, "", "", QueryOperator.NE, QueryOperator.GT,
                QueryOperator.LT);

        assertAll(true, "aaa", "aaa", QueryOperator.EQ, QueryOperator.GE,
                QueryOperator.LE);
        assertAll(false, "aaa", "aaa", QueryOperator.NE, QueryOperator.GT,
                QueryOperator.LT);

        assertAll(true, "bbb", "aaa", QueryOperator.NE, QueryOperator.GT,
                QueryOperator.GE);
        assertAll(false, "bbb", "aaa", QueryOperator.EQ, QueryOperator.LT,
                QueryOperator.LE);

        assertAll(true, "b", "aaa", QueryOperator.NE, QueryOperator.GT,
                QueryOperator.GE);
        assertAll(false, "b", "aaa", QueryOperator.EQ, QueryOperator.LT,
                QueryOperator.LE);

        assertAll(true, "aaa", "bbb", QueryOperator.NE, QueryOperator.LT,
                QueryOperator.LE);
        assertAll(false, "aaa", "bbb", QueryOperator.EQ, QueryOperator.GT,
                QueryOperator.GE);

        assertAll(true, "a", "bbb", QueryOperator.NE, QueryOperator.LT,
                QueryOperator.LE);
        assertAll(false, "a", "bbb", QueryOperator.EQ, QueryOperator.GT,
                QueryOperator.GE);
    }

    private void assertAll(boolean shouldBe, Object instance, Object target,
            QueryOperator... operators) {
        for (QueryOperator operator : operators) {
//            Assert.assertEquals(e.evaluate(operator, instance, target),
//                    shouldBe);
        }
    }
}
