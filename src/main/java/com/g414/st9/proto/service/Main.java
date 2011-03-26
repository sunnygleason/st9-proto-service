package com.g414.st9.proto.service;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.store.SqliteKeyValueStorage.SqliteKeyValueStorageModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;

/**
 * Starts the embedded Jetty server.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        if (System.getProperty("java.net.preferIPv4Stack") == null) {
            System.setProperty("java.net.preferIPv4Stack", "true");
        }

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

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { root,
                getSecondaryLogHandler(System.getProperty("log.dir", "logs")) });
        server.setHandler(handlers);

        Lifecycle lifecycle = parentInjector.getInstance(Lifecycle.class);
        lifecycle.init();
        lifecycle.start();

        server.start();
    }

    private static RequestLogHandler getSecondaryLogHandler(String localLogPath) {
        RequestLogHandler logHandler = new RequestLogHandler();
        NCSARequestLog requestLog = new NCSARequestLog(localLogPath
                + "/jetty-yyyy_mm_dd.request.log");
        requestLog.setRetainDays(180);
        requestLog.setAppend(true);
        requestLog.setExtended(true);
        requestLog.setLogLatency(true);
        requestLog.setLogTimeZone("UTC");
        logHandler.setRequestLog(requestLog);

        return logHandler;
    }

    private static Module getStorageModule() throws Exception {
        String moduleName = System.getProperty("st9.storageModule",
                SqliteKeyValueStorageModule.class.getName());

        return (Module) Class.forName(moduleName).newInstance();
    }

    private static Module getCacheModule() throws Exception {
        final String moduleName = System.getProperty("st9.cacheClass",
                EmptyKeyValueCache.class.getName());

        return new AbstractModule() {
            @Override
            protected void configure() {
                try {
                    bind(KeyValueCache.class).toInstance(
                            (KeyValueCache) Class.forName(moduleName)
                                    .newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
