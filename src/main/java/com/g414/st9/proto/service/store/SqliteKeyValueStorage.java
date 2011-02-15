package com.g414.st9.proto.service.store;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.JDBC;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.jolbox.bonecp.BoneCPDataSource;
import com.jolbox.bonecp.ConnectionHandle;
import com.jolbox.bonecp.hooks.AbstractConnectionHook;

/**
 * MySQL implementation of key-value storage using JDBI.
 */
public class SqliteKeyValueStorage extends JDBIKeyValueStorage {
	private static final Logger log = LoggerFactory
			.getLogger(SqliteKeyValueStorage.class);

	@Inject
	public SqliteKeyValueStorage(IDBI database) {
		super(database);
	}

	protected String getPrefix() {
		return "sqlite:sqlite_";
	}

	public static class SqliteKeyValueStorageModule extends AbstractModule {
		@Override
		public void configure() {
			Binder binder = binder();

			BoneCPDataSource datasource = new BoneCPDataSource();
			datasource.setDriverClass(JDBC.class.getName());
			datasource.setJdbcUrl("jdbc:sqlite:thedb.db");
			datasource.setUsername("root");
			datasource.setPassword("notreallyused");

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

			JDBIKeyValueStorage storage = new SqliteKeyValueStorage(dbi);
			binder.bind(KeyValueStorage.class).toInstance(storage);

			storage.initialize();
		}
	}
}
