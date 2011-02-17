package com.g414.st9.proto.service.store;

import java.util.List;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.Update;

public class SequenceHelper {

    public static Integer validateType(Map<String, Integer> typeCodes,
            String prefix, Handle handle, final String type, boolean doCreate) {
        if (type == null || type.length() == 0 || type.indexOf(":") != -1) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }

        if (typeCodes.containsKey(type)) {
            Integer typeId = typeCodes.get(type);

            return typeId;
        }

        Query<Map<String, Object>> query = handle.createQuery(prefix
                + "get_type_id");
        query.bind("type_name", type);
        List<Map<String, Object>> result = query.list();

        Integer typeId = null;

        if (result == null || result.isEmpty()) {
            if (!doCreate) {
                throw new WebApplicationException(Response
                        .status(Status.BAD_REQUEST)
                        .entity("Invalid entity 'type'").build());
            }

            typeId = getNextId(prefix, handle, 0, doCreate).intValue();

            Update newType = handle.createStatement(prefix + "populate_id");
            newType.bind("key_type", typeId);
            newType.bind("next_id", 1);
            newType.execute();

            Update newTypeName = handle.createStatement(prefix
                    + "populate_key_type");
            newTypeName.bind("key_type", typeId);
            newTypeName.bind("type_name", type);
            newTypeName.execute();
        } else {
            typeId = ((Number) result.iterator().next().get("_key_type"))
                    .intValue();
        }

        typeCodes.put(type, typeId);

        return typeId;
    }

    public static Long getNextId(String prefix, Handle handle, Integer typeId,
            boolean doCreate) {
        Query<Map<String, Object>> query = handle.createQuery(prefix
                + "get_next_id");
        query.bind("key_type", typeId);
        List<Map<String, Object>> result = query.list();

        if (result == null || result.isEmpty()) {
            if (!doCreate) {
                throw new WebApplicationException(Response
                        .status(Status.BAD_REQUEST)
                        .entity("Sequence not found for 'type'").build());
            }

            Update newType = handle.createStatement(prefix + "populate_id");
            newType.bind("key_type", typeId);
            newType.bind("next_id", 2);
            newType.execute();

            return 1L;
        }

        Long nextId = ((Number) result.iterator().next().get("_next_id"))
                .longValue();

        Update newType = handle.createStatement(prefix + "increment_next_id");
        newType.bind("key_type", typeId);
        newType.execute();

        return nextId;
    }
}
