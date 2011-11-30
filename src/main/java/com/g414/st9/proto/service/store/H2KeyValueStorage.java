package com.g414.st9.proto.service.store;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import com.g414.st9.proto.service.CounterResource;
import com.g414.st9.proto.service.ImportExportResource;
import com.g414.st9.proto.service.PingResource;
import com.g414.st9.proto.service.SchemaResource;
import com.g414.st9.proto.service.SecondaryIndexResource;
import com.g414.st9.proto.service.UniqueIndexResource;
import com.g414.st9.proto.service.helper.H2TypeHelper;
import com.g414.st9.proto.service.helper.JDBIHelper;
import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.index.JDBISecondaryIndex;
import com.g414.st9.proto.service.index.SecondaryIndexTableHelper;
import com.g414.st9.proto.service.sequence.SequenceHelper;
import com.g414.st9.proto.service.sequence.SequenceService;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import com.jolbox.bonecp.BoneCPDataSource;
import com.mysql.jdbc.Driver;

/**
 * H2 SQL implementation of key-value storage using JDBI.
 */
public class H2KeyValueStorage extends JDBIKeyValueStorage {
    protected String getPrefix() {
        return H2TypeHelper.DATABASE_PREFIX;
    }

    public static class H2KeyValueStorageModule extends AbstractModule {
        @Override
        public void configure() {
            Binder binder = binder();

            BoneCPDataSource datasource = new BoneCPDataSource();

            datasource.setDriverClass(Driver.class.getName());
            datasource.setJdbcUrl(System.getProperty("jdbc.url",
                    "jdbc:h2:mem:thedb;DB_CLOSE_ON_EXIT=FALSE"));
            datasource.setUsername(System.getProperty("jdbc.user", "root"));
            datasource.setPassword(System.getProperty("jdbc.password",
                    "notreallyused"));

            DBI dbi = JDBIHelper.getDBI(datasource);
            binder.bind(IDBI.class).toInstance(dbi);

            binder.bind(String.class).annotatedWith(Names.named("db.prefix"))
                    .toInstance(H2TypeHelper.DATABASE_PREFIX);

            binder.bind(Boolean.class)
                    .annotatedWith(Names.named("nuke.allowed"))
                    .toInstance(
                            Boolean.valueOf(System.getProperty("nuke.allowed",
                                    "false")));

            binder.bind(SequenceService.class).toInstance(
                    new SequenceService(new SequenceHelper(Boolean
                            .valueOf(System.getProperty("strict.type.creation",
                                    "true"))), dbi,
                            H2TypeHelper.DATABASE_PREFIX,
                            SequenceService.DEFAULT_INCREMENT));

            binder.bind(KeyValueStorage.class).to(H2KeyValueStorage.class)
                    .asEagerSingleton();
            binder.bind(SqlTypeHelper.class).to(H2TypeHelper.class)
                    .asEagerSingleton();
            binder.bind(SecondaryIndexTableHelper.class).asEagerSingleton();
            binder.bind(JDBISecondaryIndex.class).asEagerSingleton();

            bind(SchemaResource.class).asEagerSingleton();
            bind(CounterResource.class).asEagerSingleton();
            bind(SecondaryIndexResource.class).asEagerSingleton();
            bind(UniqueIndexResource.class).asEagerSingleton();
            bind(ImportExportResource.class).asEagerSingleton();
            bind(PingResource.class).asEagerSingleton();
        }
    }
}
