package com.g414.st9.proto.service.store;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.JDBC;

import com.g414.st9.proto.service.ImportExportResource;
import com.g414.st9.proto.service.SecondaryIndexResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.index.SqliteSecondaryIndex;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;
import com.jolbox.bonecp.ConnectionHandle;
import com.jolbox.bonecp.hooks.AbstractConnectionHook;

/**
 * MySQL implementation of key-value storage using JDBI.
 */
public class SqliteKeyValueStorage extends JDBIKeyValueStorage {
    private static final String DATABASE_PREFIX = "sqlite:sqlite_";
    private static final Logger log = LoggerFactory
            .getLogger(SqliteKeyValueStorage.class);

    protected String getPrefix() {
        return DATABASE_PREFIX;
    }

    @Override
    public synchronized Response create(String type, String inValue, Long id,
            boolean strictType) throws Exception {
        return super.create(type, inValue, id, strictType);
    }

    @Override
    public synchronized Response retrieve(String key) throws Exception {
        return super.retrieve(key);
    }

    @Override
    public synchronized Response multiRetrieve(List<String> keys)
            throws Exception {
        return super.multiRetrieve(keys);
    }

    @Override
    public synchronized Response update(String key, String inValue)
            throws Exception {
        return super.update(key, inValue);
    }

    @Override
    public synchronized Response delete(String key) throws Exception {
        return super.delete(key);
    }

    @Override
    public synchronized void clear(boolean preserveSchema) {
        super.clear(preserveSchema);
    }

    @Override
    public Response clearRequested(boolean preserveSchema) throws Exception {
        super.clear(preserveSchema);

        return Response.status(Status.NO_CONTENT).entity("").build();
    }

    public static class SqliteKeyValueStorageModule extends AbstractModule {
        @Override
        public void configure() {
            Binder binder = binder();

            BoneCPDataSource datasource = new BoneCPDataSource();
            datasource.setDriverClass(JDBC.class.getName());
            datasource.setJdbcUrl(System.getProperty("jdbc.url",
                    "jdbc:sqlite:thedb.db"));
            datasource.setUsername(System.getProperty("jdbc.user", "root"));
            datasource.setPassword(System.getProperty("jdbc.password",
                    "notreallyused"));

            datasource.setConnectionHook(new AbstractConnectionHook() {
                @Override
                public void onCheckOut(ConnectionHandle arg0) {
                    PreparedStatement stmt = null;
                    try {
                        stmt = arg0
                                .prepareStatement("pragma synchronous = off");
                        stmt.execute();
                    } catch (SQLException e) {
                        log.warn(
                                "Error while setting pragma " + e.getMessage(),
                                e);

                        throw new RuntimeException(e);
                    } finally {
                        if (stmt != null) {
                            try {
                                stmt.close();
                            } catch (SQLException e) {
                                log.warn(
                                        "Error while setting pragma "
                                                + e.getMessage(), e);

                                throw new RuntimeException(e);
                            }
                        }
                    }
                }
            });

            DBI dbi = JDBIHelper.getDBI(datasource);
            binder.bind(IDBI.class).toInstance(dbi);

            binder.bind(String.class).annotatedWith(Names.named("db.prefix"))
                    .toInstance(DATABASE_PREFIX);

            binder.bind(CounterService.class).toInstance(
                    new CounterService(dbi, DATABASE_PREFIX));

            binder.bind(KeyValueStorage.class).to(SqliteKeyValueStorage.class)
                    .asEagerSingleton();
            binder.bind(JDBISecondaryIndex.class)
                    .to(SqliteSecondaryIndex.class).asEagerSingleton();

            bind(SchemaResource.class).asEagerSingleton();
            bind(SecondaryIndexResource.class).asEagerSingleton();
            bind(ImportExportResource.class).asEagerSingleton();
        }
    }

}
