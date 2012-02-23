package com.g414.st9.proto.service.helper;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class AvailabilityManager implements Releasable {
    private final AtomicBoolean available = new AtomicBoolean();

    public <T> T doProtected(ProtectedCommand<T> command) {
        boolean allowed = available.compareAndSet(true, false);
        try {
            if (allowed) {
                return command.execute(AvailabilityManager.this);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        throw new IllegalStateException("service down for maintenance");
    }

    public void setAvailable(boolean availability) {
        available.set(availability);
    }

    public boolean isAvailable() {
        return available.get();
    }

    public boolean release() {
        return available.compareAndSet(false, true);
    }

    public Response unavailableResponse() {
        return Response.status(Status.SERVICE_UNAVAILABLE)
                .entity("service down for maintenance").build();
    }

    public static void sendUnavailableResponse(ServletResponse response)
            throws IOException {
        HttpServletResponse resp = (HttpServletResponse) response;

        resp.setStatus(Status.SERVICE_UNAVAILABLE.getStatusCode());

        PrintWriter writer = null;
        try {
            writer = resp.getWriter();
            writer.println("service down for maintenance");
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    public interface ProtectedCommand<T> {
        public T execute(Releasable resource) throws Exception;
    }
}
