package utest.com.g414.st9.proto.service.query;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.query.QueryLexer;
import com.g414.st9.proto.service.query.QueryParser;
import com.g414.st9.proto.service.query.QueryParser.term_list_return;
import com.g414.st9.proto.service.query.QueryTerm;

@Test
public class QueryTest {
    public void testSimpleQuery() throws Exception {
        String query = "foo eq -1 and _x ne \"wha\" and _bar_val gt 9.91";

        parseQuery(query);
    }

    public void testDoubleQuotes() throws Exception {
        String query = "_x eq \"\\\"\"";

        QueryLexer lex = new QueryLexer(new ANTLRStringStream(query));
        CommonTokenStream tokens = new CommonTokenStream(lex);

        QueryParser parser = new QueryParser(tokens);
        parser.setTreeAdaptor(adaptor);

        List<QueryTerm> foo = new ArrayList<QueryTerm>();
        parser.term_list(foo);

        Assert.assertEquals(foo.toString(), "[_x EQ \"\"\"]");
        Assert.assertTrue(foo.size() == 1);
    }

    static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
        public Object create(Token payload) {
            return new CommonTree(payload);
        }
    };

    private Object parseQuery(String query) throws RecognitionException {
        QueryLexer lex = new QueryLexer(new ANTLRStringStream(query));
        CommonTokenStream tokens = new CommonTokenStream(lex);

        QueryParser parser = new QueryParser(tokens);
        parser.setTreeAdaptor(adaptor);

        List<QueryTerm> foo = new ArrayList<QueryTerm>();
        term_list_return x = parser.term_list(foo);

        Assert.assertEquals(foo.toString(),
                "[foo EQ -1, _x NE \"wha\", _bar_val GT 9.91]");

        Assert.assertTrue(foo.size() == 3);

        return x;
    }
}
