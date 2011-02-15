package com.g414.st9.proto.service;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.g414.st9.proto.service.store.SqliteKeyValueStorage.SqliteKeyValueStorageModule;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;

/**
 * Starts the embedded Jetty server.
 */
public class Main {
	public static void main(String[] args) throws Exception {
		int port = (args.length == 1) ? Integer.parseInt(args[0]) : 8080;

		Server server = new Server(port);
		ServletContextHandler root = new ServletContextHandler(server, "/",
				ServletContextHandler.NO_SESSIONS);

		Module storageModule = getStorageModule();

		root.addEventListener(new ServiceConfig(storageModule));
		root.addFilter(GuiceFilter.class, "/*", 0);
		root.addServlet(EmptyServlet.class, "/*");

		server.start();
	}

	private static Module getStorageModule() throws Exception {
		String moduleName = System.getProperty("st9.storageModule",
				SqliteKeyValueStorageModule.class.getName());

		return (Module) Class.forName(moduleName).newInstance();
	}
}
