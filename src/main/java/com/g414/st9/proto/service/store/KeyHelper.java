package com.g414.st9.proto.service.store;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class KeyHelper {
    public static Object[] validateKey(String key) {
        if (key == null || key.length() == 0 || key.indexOf(":") == -1) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'id'")
                    .build());
        }

        String[] parts = key.split(":");
        if (parts.length != 2) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'id'")
                    .build());
        }

        try {
            return new Object[] { parts[0], Long.parseLong(parts[1]) };
        } catch (NumberFormatException e) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'id'")
                    .build());
        }
    }
}
