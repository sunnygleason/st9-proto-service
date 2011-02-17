package com.g414.st9.proto.service;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache.EmptyKeyValueCacheModule;
import com.g414.st9.proto.service.store.SqliteKeyValueStorage.SqliteKeyValueStorageModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
        Module cacheModule = getCacheModule();

        Injector parentInjector = Guice.createInjector(new LifecycleModule(),
                storageModule, cacheModule, new ServiceModule());

        root.addFilter(GuiceFilter.class, "/*", 0);
        root.addServlet(EmptyServlet.class, "/*");

        Lifecycle lifecycle = parentInjector.getInstance(Lifecycle.class);
        lifecycle.init();
        lifecycle.start();

        server.start();
    }

    private static Module getStorageModule() throws Exception {
        String moduleName = System.getProperty("st9.storageModule",
                SqliteKeyValueStorageModule.class.getName());

        return (Module) Class.forName(moduleName).newInstance();
    }

    private static Module getCacheModule() throws Exception {
        String moduleName = System.getProperty("st9.cacheModule",
                EmptyKeyValueCacheModule.class.getName());

        return (Module) Class.forName(moduleName).newInstance();
    }
}
