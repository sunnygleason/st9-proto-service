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
    public static Integer validateType(Handle handle, String prefix,
            Map<String, Integer> typeCodes, Map<Integer, String> typeNames,
            final String type, boolean doCreate) {
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

            typeId = getNextId(handle, prefix, 0, 1L).intValue();

            Update newType = handle.createStatement(prefix
                    + "insert_ignore_seq");
            newType.bind("key_type", typeId);
            newType.bind("next_id", 0L);
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
        typeNames.put(typeId, type);

        return typeId;
    }

    public static synchronized Long getNextId(Handle handle, String prefix,
            Integer typeId, Long increment) {
        Query<Map<String, Object>> query = handle.createQuery(prefix
                + "get_next_id");
        query.bind("key_type", typeId);

        Long nextId = ((Number) query.first().get("_next_id")).longValue();

        Update incrSeq = handle.createStatement(prefix + "increment_next_id");
        incrSeq.bind("key_type", typeId);
        incrSeq.bind("increment", increment);
        incrSeq.execute();

        return nextId;
    }

    public static String getTypeName(Handle handle, String prefix,
            Map<Integer, String> typeNames, Integer id) {
        if (id == null || id < 0) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }

        if (typeNames.containsKey(id)) {
            return typeNames.get(id);
        }

        Query<Map<String, Object>> query = handle.createQuery(prefix
                + "get_type_name");
        query.bind("key_type", id);
        List<Map<String, Object>> result = query.list();

        if (result == null || result.isEmpty()) {
            throw new WebApplicationException(Response
                    .status(Status.BAD_REQUEST).entity("Invalid entity 'type'")
                    .build());
        }

        String typeName = (String) result.iterator().next().get("_type_name");
        typeNames.put(id, typeName);

        return typeName;
    }
}
