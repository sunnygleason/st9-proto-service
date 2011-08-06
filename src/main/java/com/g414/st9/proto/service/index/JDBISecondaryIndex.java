package com.g414.st9.proto.service.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.Binding;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.NoOpStatementRewriter;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.tweak.RewrittenStatement;
import org.skife.jdbi.v2.tweak.StatementRewriter;

import com.g414.st9.proto.service.helper.JDBIHelper;
import com.g414.st9.proto.service.helper.SqlParamBindings;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.schema.IndexAttribute;
import com.g414.st9.proto.service.schema.IndexDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.google.inject.Inject;

public class JDBISecondaryIndex {
	private final SecondaryIndexTableHelper tableHelper;

	@Inject
	public JDBISecondaryIndex(SecondaryIndexTableHelper tableHelper) {
		this.tableHelper = tableHelper;
	}

	public void createTable(IDBI database, final String type,
			final String indexName, final SchemaDefinition schemaDefinition) {
		JDBIHelper.createTable(database, tableHelper.getTableDrop(type,
				indexName), tableHelper.getTableDefinition(type, indexName,
				schemaDefinition));
	}

	public void createIndex(IDBI database, final String type,
			final String indexName, final SchemaDefinition schemaDefinition) {
		database.inTransaction(new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Handle handle, TransactionStatus arg1)
					throws Exception {
				try {
					handle.createStatement(
							tableHelper.getPrefix() + "drop_index")
							.define("table_name",
									tableHelper.getTableName(type, indexName))
							.define("index_name",
									tableHelper.getIndexName(type, indexName))
							.execute();
				} catch (UnableToExecuteStatementException ok) {
					// expected case in mysql - this is just best-effort anyway
				}

				handle.createStatement(
						tableHelper.getIndexDefinition(type, indexName,
								schemaDefinition)).execute();

				return null;
			}
		});
	}

	public void dropTableAndIndex(Handle handle, final String type,
			final String indexName) {
		handle.createStatement(tableHelper.getTableDrop(type, indexName))
				.execute();

		try {
			handle.createStatement(tableHelper.getPrefix() + "drop_index")
					.define("table_name",
							tableHelper.getTableName(type, indexName))
					.define("index_name",
							tableHelper.getIndexName(type, indexName))
					.execute();
		} catch (UnableToExecuteStatementException ok) {
			// expected case in mysql - this is just best-effort anyway
		}
	}

	public void dropTable(IDBI database, final String type,
			final String indexName) {
		database.inTransaction(new TransactionCallback<Void>() {
			@Override
			public Void inTransaction(Handle handle, TransactionStatus arg1)
					throws Exception {
				dropTableAndIndex(handle, type, indexName);

				return null;
			}
		});
	}

	public void insertEntity(Handle handle, final Long id,
			final Map<String, Object> value, final String type,
			final String indexName, final SchemaDefinition schemaDefinition) {
		SqlParamBindings bindings = new SqlParamBindings(true);

		Update insert = handle.createStatement(tableHelper.getInsertStatement(
				type, indexName, schemaDefinition, bindings));

		IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
				indexName);

		for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
			String attrName = attr.getName();
			if ("id".equals(attrName)) {
				bindings.bind("id", id);
			} else {
				Object v = value.get(attrName) != null ? value.get(attrName)
						.toString() : null;

				bindings.bind(attrName,
						tableHelper.transformAttributeValue(v, attr));
			}
		}

		bindings.bindToStatement(insert);

		try {
			insert.execute();
		} catch (UnableToExecuteStatementException e) {
			if (tableHelper.isConstraintViolation(e)) {
				throw new WebApplicationException(Response
						.status(Status.CONFLICT)
						.entity("unique index constraint violation").build());
			} else {
				throw e;
			}
		}
	}

	public void updateEntity(Handle handle, final Long id,
			final Map<String, Object> value, final String type,
			final String indexName, final SchemaDefinition schemaDefinition) {
		SqlParamBindings bindings = new SqlParamBindings(true);

		Update update = handle.createStatement(tableHelper.getUpdateStatement(
				type, indexName, schemaDefinition, bindings));

		IndexDefinition indexDefinition = schemaDefinition.getIndexMap().get(
				indexName);

		for (IndexAttribute attr : indexDefinition.getIndexAttributes()) {
			String attrName = attr.getName();
			if ("id".equals(attrName)) {
				bindings.bind("id", id);
			} else {
				Object v = value.get(attrName) != null ? value.get(attrName)
						.toString() : null;

				bindings.bind(attrName,
						tableHelper.transformAttributeValue(v, attr));
			}
		}

		bindings.bindToStatement(update);

		try {
			update.execute();
		} catch (UnableToExecuteStatementException e) {
			if (tableHelper.isConstraintViolation(e)) {
				throw new WebApplicationException(Response
						.status(Status.CONFLICT)
						.entity("unique index constraint violation").build());
			} else {
				throw e;
			}
		}
	}

	public void deleteEntity(Handle handle, final Long id, final String type,
			final String indexName) {
		SqlParamBindings bindings = new SqlParamBindings(true);

		Update delete = handle.createStatement(tableHelper.getDeleteStatement(
				type, indexName, bindings));

		bindings.bind("id", id);
		bindings.bindToStatement(delete);

		delete.execute();
	}

	public void clear(Handle handle, Iterator<Map<String, Object>> schemas,
			boolean preserveSchema) throws Exception {
		while (schemas.hasNext()) {
			Map<String, Object> schema = schemas.next();
			if (schema == null) {
				continue;
			}

			String type = (String) schema.get("$type");

			if (schema != null && schema.get("indexes") != null) {
				for (Map<String, Object> index : (List<Map<String, Object>>) schema
						.get("indexes")) {
					String indexName = (String) index.get("name");

					if (preserveSchema) {
						this.truncateTable(handle, type, indexName);
					} else {
						this.dropTableAndIndex(handle, type, indexName);
					}
				}
			}
		}
	}

	public List<Map<String, Object>> doIndexQuery(IDBI database, String type,
			String indexName, List<QueryTerm> queryTerms, String token,
			Long pageSize, SchemaDefinition schemaDefinition) throws Exception {
		final List<Map<String, Object>> resultIds = new ArrayList<Map<String, Object>>();
		final SqlParamBindings bindings = new SqlParamBindings(true);

		final String querySql = tableHelper.getIndexQuery(type, indexName,
				queryTerms, token, pageSize, schemaDefinition, bindings);

		Response response = database
				.inTransaction(new TransactionCallback<Response>() {
					@Override
					public Response inTransaction(Handle handle,
							TransactionStatus status) throws Exception {
						try {
							Query<Map<String, Object>> query = handle
									.createQuery(querySql);

							bindings.bindToStatement(query);

							for (Map<String, Object> r : query.list()) {
								resultIds.add(r);
							}

							return null;
						} catch (WebApplicationException e) {
							return e.getResponse();
						}
					}
				});

		if (response != null) {
			throw new WebApplicationException(response);
		}

		return Collections.unmodifiableList(resultIds);
	}

	public boolean tableExists(IDBI database, final String type,
			final String indexName) {
		return JDBIHelper.tableExists(database, tableHelper.getPrefix(),
				tableHelper.getTableName(type, indexName));
	}

	public void truncateTable(Handle handle, final String type,
			final String indexName) {
		String indexTableName = tableHelper.getTableName(type, indexName);
		handle.createStatement(tableHelper.getPrefix() + "truncate_table")
				.define("table_name", indexTableName).execute();
	}
}
