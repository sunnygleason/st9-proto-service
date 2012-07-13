package com.g414.st9.proto.service;

import java.net.InetAddress;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.g414.guice.lifecycle.Lifecycle;
import com.g414.guice.lifecycle.LifecycleModule;
import com.g414.st9.proto.service.cache.EmptyKeyValueCache;
import com.g414.st9.proto.service.cache.KeyValueCache;
import com.g414.st9.proto.service.helper.EmptyServlet;
import com.g414.st9.proto.service.helper.ExtendedHttpRequestLogV20110917;
import com.g414.st9.proto.service.pubsub.CubePublisher;
import com.g414.st9.proto.service.pubsub.NoOpPublisher;
import com.g414.st9.proto.service.pubsub.Publisher;
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

        int port = Integer.parseInt(System.getProperty("http.port", "8080"));

        Server server = new Server();

        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(port);
        connector.setThreadPool(new QueuedThreadPool(200));
        connector.setAcceptors(4);
        connector.setMaxIdleTime(300000);
        connector.setAcceptQueueSize(12000);
        connector.setLowResourcesConnections(25000);

        server.setConnectors(new Connector[] { connector });

        ServletContextHandler root = new ServletContextHandler(server, "/",
                ServletContextHandler.NO_SESSIONS);

        Module storageModule = getStorageModule();
        Module cacheModule = getCacheModule();
        Module publisherModule = getPublisherModule();

        Injector parentInjector = Guice.createInjector(new LifecycleModule(),
                storageModule, cacheModule, publisherModule,
                new ServiceModule());

        root.addFilter(GuiceFilter.class, "/*", 0);
        root.addServlet(EmptyServlet.class, "/*");

        HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { root,
                getSecondaryLogHandler(System.getProperty("log.dir", "logs")) });
        server.setHandler(handlers);

        final Lifecycle lifecycle = parentInjector.getInstance(Lifecycle.class);
        lifecycle.init();
        lifecycle.start();

        addShutdownHook(lifecycle);

        server.getThreadPool();
        server.start();
    }

    private static RequestLogHandler getSecondaryLogHandler(String localLogPath) {
        RequestLogHandler logHandler = new RequestLogHandler();
        ExtendedHttpRequestLogV20110917 requestLog = new ExtendedHttpRequestLogV20110917(
                localLogPath + "/jetty-" + getHostname()
                        + "-yyyy_mm_dd.request.log");
        logHandler.setRequestLog(requestLog);

        return logHandler;
    }

    private static Module getStorageModule() throws Exception {
        String moduleName = System.getProperty("st9.storageModule",
                SqliteKeyValueStorageModule.class.getName());

        return (Module) Class.forName(moduleName).newInstance();
    }

    private static String getHostname() {
        try {
            InetAddress addr = InetAddress.getLocalHost();

            return addr.getHostName().replaceAll("\\W", "_");
        } catch (Exception e) {
            e.printStackTrace();

            return "localhost";
        }
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

    private static Module getPublisherModule() throws Exception {
        final String moduleName = System.getProperty("st9.publisherClass",
                NoOpPublisher.class.getName());

        return new AbstractModule() {
            @Override
            protected void configure() {
                try {
                    bind(Publisher.class)
                            .toInstance(
                                    (Publisher) Class.forName(moduleName)
                                            .newInstance());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private static void addShutdownHook(final Lifecycle lifecycle) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.err
                        .println("Shutdown request received: notifying lifecycle");
                lifecycle.shutdown();
                System.err.println("Lifecycle notified: waiting 3s");

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }

                System.err.println("Clean shutdown.");
            }
        }));
    }
}
