package utest.com.g414.st9.proto.service.schema;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.schema.SchemaDefinition;

@Test
public class TestSchema {
    private final String schema1 = "{\"attributes\":[{\"name\":\"isAwesome\",\"type\":\"BOOLEAN\"}],\"indexes\":[]}";
    private final ObjectMapper mapper = new ObjectMapper();

    public void testSchema1() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema1, SchemaDefinition.class);

        String redux = mapper.writeValueAsString(def);
        System.out.println(redux);

        Assert.assertEquals(redux, schema1);
    }
}
