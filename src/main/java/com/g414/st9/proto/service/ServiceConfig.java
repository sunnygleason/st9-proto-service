package com.g414.st9.proto.service;

import com.google.inject.Guice;
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
	@Override
	protected Injector getInjector() {
		return Guice.createInjector(new ServletModule() {
			@Override
			protected void configureServlets() {
				bind(InMemoryKeyValueStorage.class).asEagerSingleton();
				bind(KeyValueResource.class).asEagerSingleton();

				serve("*").with(GuiceContainer.class);
			}
		});
	}
}
