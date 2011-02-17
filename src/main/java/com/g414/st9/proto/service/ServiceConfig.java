package com.g414.st9.proto.service;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * Guice Service configuration. Creates an Injector, and binds it to whatever
 * Modules we want. In this case, we use an inner static Module class, but other
 * modules could be welcome as well.
 */
public class ServiceConfig extends GuiceServletContextListener {
    private final Injector parentInjector;

    public ServiceConfig(Injector parentInjector) {
        this.parentInjector = parentInjector;
    }

    @Override
    protected Injector getInjector() {
        return parentInjector.createChildInjector(new ServiceConfigModule());
    }

    public static class ServiceConfigModule extends ServletModule {
        protected void configureServlets() {
            bind(KeyValueResource.class).asEagerSingleton();

            serve("*").with(GuiceContainer.class);
        }
    }
}
