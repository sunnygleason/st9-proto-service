package com.g414.st9.proto.service.store;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.jolbox.bonecp.BoneCPDataSource;
import com.mysql.jdbc.Driver;

/**
 * MySQL implementation of key-value storage using JDBI.
 */
public class MySQLKeyValueStorage extends JDBIKeyValueStorage {
	public MySQLKeyValueStorage(IDBI database) {
		super(database);
	}

	protected String getPrefix() {
		return "mysql:mysql_";
	}

	public static class MySQLKeyValueStorageModule extends AbstractModule {
		@Override
		public void configure() {
			Binder binder = binder();

			BoneCPDataSource datasource = new BoneCPDataSource();
			datasource.setDriverClass(Driver.class.getName());
			datasource.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/thedb");
			datasource.setUsername("root");
			datasource.setPassword("");

			DBI dbi = JDBIHelper.getDBI(datasource);
			binder.bind(IDBI.class).toInstance(dbi);

			JDBIKeyValueStorage storage = new MySQLKeyValueStorage(dbi);
			binder.bind(KeyValueStorage.class).toInstance(storage);

			storage.initialize();
		}
	}
}
