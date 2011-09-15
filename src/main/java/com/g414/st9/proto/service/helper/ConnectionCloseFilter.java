package com.g414.st9.proto.service.helper;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

/**
 * This filter prevents clients from using keep-alive
 */
public class ConnectionCloseFilter implements Filter {
    @Override
    public void init(FilterConfig unused) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        if (response instanceof HttpServletResponse) {
            ((HttpServletResponse) response).setHeader("Connection", "close");
        }

        chain.doFilter(request, response);
    }
}
