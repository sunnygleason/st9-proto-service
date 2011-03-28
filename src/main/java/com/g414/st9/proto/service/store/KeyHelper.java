package com.g414.st9.proto.service.store;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class KeyHelper {
    public static void validateKey(String key) {
        if (key == null || key.length() == 0 || key.indexOf(":") == -1) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid key").build());
        }

        String[] parts = key.split(":");
        if (parts.length != 2) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid key").build());
        }

        try {
            Key.valueOf(key);
        } catch (Exception e) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid key").build());
        }
    }
}
