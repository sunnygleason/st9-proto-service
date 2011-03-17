package com.g414.st9.proto.service.store;

import javax.ws.rs.core.Response;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import com.g414.st9.proto.service.RealRelationalIndexResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.index.MySQLSecondaryIndex;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.jolbox.bonecp.BoneCPDataSource;
import com.mysql.jdbc.Driver;

/**
 * MySQL implementation of key-value storage using JDBI.
 */
public class MySQLKeyValueStorage extends JDBIKeyValueStorage {
    protected String getPrefix() {
        return "mysql:mysql_";
    }

    @Override
    public Response clearRequested() throws Exception {
        throw new UnsupportedOperationException();
    }

    public static class MySQLKeyValueStorageModule extends AbstractModule {
        @Override
        public void configure() {
            Binder binder = binder();

            BoneCPDataSource datasource = new BoneCPDataSource();
            datasource.setDriverClass(Driver.class.getName());
            datasource.setJdbcUrl(System.getProperty("jdbc.url",
                    "jdbc:mysql://127.0.0.1:3306/thedb"));
            datasource.setUsername(System.getProperty("jdbc.user", "root"));
            datasource.setPassword(System.getProperty("jdbc.password", ""));

            DBI dbi = JDBIHelper.getDBI(datasource);
            binder.bind(IDBI.class).toInstance(dbi);

            binder.bind(KeyValueStorage.class).to(MySQLKeyValueStorage.class)
                    .asEagerSingleton();
            binder.bind(JDBISecondaryIndex.class).to(MySQLSecondaryIndex.class)
                    .asEagerSingleton();

            bind(SchemaResource.class).asEagerSingleton();
            bind(RealRelationalIndexResource.class).asEagerSingleton();
        }
    }
}
