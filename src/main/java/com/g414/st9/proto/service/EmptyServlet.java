package com.g414.st9.proto.service;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * An Empty, "do nothing servlet" that never gets called. But, it's necessary to
 * register at least one servlet - otherwise, the guice and jersey filters will
 * never kick in. Nothing to see here...
 */
public class EmptyServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		throw new IllegalStateException("unable to service request");
	}
}
