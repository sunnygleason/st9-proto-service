package com.g414.st9.proto.service.store;

import javax.sql.DataSource;

import org.antlr.stringtemplate.language.AngleBracketTemplateLexer;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.unstable.stringtemplate.ClasspathGroupLoader;
import org.skife.jdbi.v2.unstable.stringtemplate.StringTemplateStatementLocator;

public class JDBIHelper {
	public static DBI getDBI(DataSource datasource) {
		DBI dbi = new DBI(datasource);

		final ClasspathGroupLoader loader = new ClasspathGroupLoader(
				AngleBracketTemplateLexer.class, JDBIHelper.class.getPackage()
						.getName().replaceAll("\\.", "/"));

		dbi.setStatementLocator(new StringTemplateStatementLocator(loader));

		return dbi;
	}
}
