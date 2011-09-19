package com.g414.st9.proto.service;

import com.g414.st9.proto.service.helper.ConnectionCloseFilter;
import com.g414.st9.proto.service.helper.ExtendedRequestFilter;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * Guice Service configuration.
 */
public class ServiceModule extends ServletModule {
    @Override
    protected void configureServlets() {
        bind(KeyValueResource.class).asEagerSingleton();
        bind(NukeResource.class).asEagerSingleton();
        bind(ConnectionCloseFilter.class).asEagerSingleton();
        bind(ExtendedRequestFilter.class).asEagerSingleton();

        serve("*").with(GuiceContainer.class);

        filter("*").through(ExtendedRequestFilter.class);
        filter("*").through(ConnectionCloseFilter.class);
    }
}
