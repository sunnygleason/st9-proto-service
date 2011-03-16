package com.g414.st9.proto.service;

import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * Guice Service configuration.
 */
public class ServiceModule extends ServletModule {
    @Override
    protected void configureServlets() {
        bind(KeyValueResource.class).asEagerSingleton();
        bind(ExportKeyValueResource.class).asEagerSingleton();
        bind(FakeRelationalIndexResource.class).asEagerSingleton();

        serve("*").with(GuiceContainer.class);
    }
}
