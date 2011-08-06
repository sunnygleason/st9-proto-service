package com.g414.st9.proto.service.helper;

import java.util.StringTokenizer;

import javax.sql.DataSource;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.StringTemplateGroupLoader;
import org.antlr.stringtemplate.language.AngleBracketTemplateLexer;
import org.skife.jdbi.v2.ClasspathStatementLocator;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.UnableToCreateStatementException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.tweak.StatementLocator;

import com.g414.st9.proto.service.store.JDBIKeyValueStorage;

public class JDBIHelper {
    public static DBI getDBI(DataSource datasource) {
        DBI dbi = new DBI(datasource);

        final ClasspathGroupLoader theLoader = new ClasspathGroupLoader(
                AngleBracketTemplateLexer.class, JDBIKeyValueStorage.class
                        .getPackage().getName().replaceAll("\\.", "/"));

        dbi.setStatementLocator(new StatementLocator() {
            private final StringTemplateGroupLoader loader = theLoader;

            public String locate(String name, StatementContext ctx)
                    throws Exception {
                if (ClasspathStatementLocator.looksLikeSql(name)) {
                    return name;
                }
                final StringTokenizer tok = new StringTokenizer(name, ":");
                final String group_name = tok.nextToken();
                final String template_name = tok.nextToken();
                final StringTemplateGroup group = loader.loadGroup(group_name);
                final StringTemplate template = group
                        .getInstanceOf(template_name);

                template.setAttributes(ctx.getAttributes());
                return template.toString();
            }
        });

        return dbi;
    }

    public static void createTable(IDBI database, final String tableDrop,
            final String tableDefinition) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                handle.createStatement(tableDrop).execute();

                handle.createStatement(tableDefinition).execute();

                return null;
            }
        });
    }

    public static void dropTable(IDBI database, final String tableDrop) {
        database.inTransaction(new TransactionCallback<Void>() {
            @Override
            public Void inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                dropTable(handle, tableDrop);

                return null;
            }
        });
    }

    public static void dropTable(Handle handle, final String tableDrop) {
        handle.createStatement(tableDrop).execute();
    }

    public static boolean tableExists(IDBI database, final String prefix,
            final String tableName) {
        return database.inTransaction(new TransactionCallback<Boolean>() {
            @Override
            public Boolean inTransaction(Handle handle, TransactionStatus arg1)
                    throws Exception {
                try {
                    handle.createStatement(prefix + "table_exists")
                            .define("table_name", tableName).execute();

                    return true;
                } catch (UnableToExecuteStatementException e) {
                    // expected in missing case
                } catch (UnableToCreateStatementException e) {
                    // expected in missing case
                }

                return false;
            }
        });
    }
}
