package com.g414.st9.proto.service;

import com.g414.st9.proto.service.helper.AvailabilityManagementFilter;
import com.g414.st9.proto.service.helper.AvailabilityManager;
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
        bind(AvailabilityResource.class).asEagerSingleton();
        bind(KeyValueResource.class).asEagerSingleton();
        bind(QuarantineResource.class).asEagerSingleton();
        bind(NukeResource.class).asEagerSingleton();
        bind(AvailabilityManager.class).asEagerSingleton();
        bind(AvailabilityManagementFilter.class).asEagerSingleton();
        bind(ConnectionCloseFilter.class).asEagerSingleton();
        bind(ExtendedRequestFilter.class).asEagerSingleton();

        serve("*").with(GuiceContainer.class);

        filter("*").through(AvailabilityManagementFilter.class);
        filter("*").through(ExtendedRequestFilter.class);
        filter("*").through(ConnectionCloseFilter.class);
    }
}
