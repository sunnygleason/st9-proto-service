package com.g414.st9.proto.service.helper;

import java.io.IOException;
import java.util.UUID;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This filter decorates the request & response with useful info.
 */
public class ExtendedRequestFilter implements Filter {
    public static final String X_REQUEST_ID = "X-Request-ID";
    public static final String T1_NANOS = "t1_nanos";

    @Override
    public void init(FilterConfig unused) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            ((HttpServletRequest) request).setAttribute(T1_NANOS,
                    System.nanoTime());
        }

        if (response instanceof HttpServletResponse) {
            ((HttpServletResponse) response).setHeader(X_REQUEST_ID, UUID
                    .randomUUID().toString());
        }

        chain.doFilter(request, response);
    }
}
