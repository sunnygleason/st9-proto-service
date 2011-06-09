package com.g414.st9.proto.service.helper;

import javax.sql.DataSource;

import org.antlr.stringtemplate.language.AngleBracketTemplateLexer;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.UnableToCreateStatementException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.unstable.stringtemplate.ClasspathGroupLoader;
import org.skife.jdbi.v2.unstable.stringtemplate.StringTemplateStatementLocator;

import com.g414.st9.proto.service.store.JDBIKeyValueStorage;

public class JDBIHelper {
    public static DBI getDBI(DataSource datasource) {
        DBI dbi = new DBI(datasource);

        final ClasspathGroupLoader loader = new ClasspathGroupLoader(
                AngleBracketTemplateLexer.class, JDBIKeyValueStorage.class
                        .getPackage().getName().replaceAll("\\.", "/"));

        dbi.setStatementLocator(new StringTemplateStatementLocator(loader));

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
