package utest.com.g414.st9.proto.query;

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.query.QueryLexer;
import com.g414.st9.proto.service.query.QueryParser;
import com.g414.st9.proto.service.query.QueryParser.term_list_return;
import com.g414.st9.proto.service.query.QueryTerm;

@Test
public class QueryTest {
    public void testSimpleQuery() throws Exception {
        String query = "foo eq -1 and x ne \"wha\" and bar gt 9.91";

        parseQuery(query);
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

        System.out.println(foo);

        printTree((CommonTree) x.getTree(), 2);

        return x;
    }

    private void printTree(CommonTree t, int indent) {
        System.out.println(t.getType() + " " + t.getChildCount());
        if (t != null) {
            StringBuffer sb = new StringBuffer(indent);
            for (int i = 0; i < indent; i++)
                sb = sb.append("   ");
            for (int i = 0; i < t.getChildCount(); i++) {
                System.out.println(sb.toString() + t.getChild(i).toString());
                printTree((CommonTree) t.getChild(i), indent + 1);
            }
        }
    }

}
