package com.g414.st9.proto.service.helper;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.inject.Inject;

/**
 * This filter provides availability management capabilities
 */
public class AvailabilityManagementFilter implements Filter {
    @Inject
    private AvailabilityManager availability;

    @Override
    public void init(FilterConfig unused) throws ServletException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
            FilterChain chain) throws IOException, ServletException {
        if (!availability.isAvailable()) {
            HttpServletRequest req = (HttpServletRequest) request;
            String forceParam = req.getParameter("__FORCE__");
            boolean forced = forceParam != null && Boolean.valueOf(forceParam);

            if (!forced) {
                AvailabilityManager
                        .sendUnavailableResponse((HttpServletResponse) response);

                return;
            }
        }

        chain.doFilter(request, response);
    }

}
