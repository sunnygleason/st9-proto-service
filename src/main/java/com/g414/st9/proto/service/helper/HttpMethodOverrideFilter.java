package com.g414.st9.proto.service.helper;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;

/**
 * Clients may override the HTTP method by setting either the
 * X-HTTP-Method-Override header or the _method form or query parameter in a
 * POST request. If both the X-HTTP-Method-Override header and _method parameter
 * are present in the request then the X-HTTP-Method-Override header will be
 * used.
 */
public class HttpMethodOverrideFilter implements ContainerRequestFilter {
    private static final String HEADER = "X-HTTP-Method-Override";
    public static final String METHOD = "_method";

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        if (request.getMethod().equalsIgnoreCase("POST")) {
            String methodOverride = getFirstNonEmpty(request
                    .getRequestHeaders().getFirst(HEADER), request
                    .getFormParameters().getFirst(METHOD), request
                    .getQueryParameters().getFirst(METHOD));

            if (methodOverride != null) {
                request.setMethod(methodOverride);
            }
        }

        return request;
    }

    private String getFirstNonEmpty(String... values) {
        for (String value : values) {
            if (value != null && value.length() > 0) {
                return value;
            }
        }

        return null;
    }
}
