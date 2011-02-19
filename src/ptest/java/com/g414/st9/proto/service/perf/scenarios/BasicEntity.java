package com.g414.st9.proto.service.perf.scenarios;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.g414.dgen.EntityGenerator;
import com.g414.dgen.field.Field;
import com.g414.dgen.field.FieldBase;
import com.g414.dgen.field.Fields;
import com.g414.hash.impl.MurmurHash;

public class BasicEntity {
    public static EntityGenerator createGenerator() {
        List<Field<?>> fields = new ArrayList<Field<?>>();
        fields.add(Fields.getIdField("idKey"));
        fields.add((Field<?>) new FieldBase<String>("kind") {
            @Override
            public String getValue(String name, Map<String, Object> entity,
                    Random rand) {
                long realId = Long.parseLong((String) entity.get("idKey"));
                int type = (int) (realId % 20);

                return "type" + type;
            }
        });
        fields.add(new FieldBase<String>("id") {
            @Override
            public String getValue(String name, Map<String, Object> entity,
                    Random rand) {
                long realId = Long.parseLong((String) entity.get("idKey"));
                int id = (int) (realId / 20) + 1;

                return entity.get("kind") + ":" + id;
            }
        });
        fields.add(Fields.getBooleanField("isAwesome"));
        fields.add(Fields.getIntField("age", 1, 100));
        fields.add(Fields.getLongField("created", System.currentTimeMillis(),
                System.currentTimeMillis() + 1000000));
        fields.add(Fields.getLongField("updated",
                System.currentTimeMillis() + 1000001,
                System.currentTimeMillis() + 2000000));
        fields.add(Fields.getRandomHexBytesField("data", 140));

        return new EntityGenerator(new MurmurHash(), fields);
    }
}
