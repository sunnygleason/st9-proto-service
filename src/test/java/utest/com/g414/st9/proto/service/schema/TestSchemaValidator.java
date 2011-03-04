package utest.com.g414.st9.proto.service.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.google.common.collect.ImmutableMap;

@Test
public class TestSchemaValidator {
    private final String schema1 = "{\"attributes\":[{\"name\":\"isAwesome\",\"type\":\"BOOLEAN\"},{\"name\":\"hotness\",\"type\":\"ENUM\",\"values\":[\"FREEZING\",\"COLD\",\"COOL\",\"WARM\",\"TEH_HOTNESS\"]}],\"indexes\":[]}";
    private final String schema2 = "{\"attributes\":[{\"name\":\"when\",\"type\":\"UTC_DATE_SECS\"},{\"name\":\"lname\",\"type\":\"UTF8_SMALLSTRING\"}],\"indexes\":[]}";
    private final String schema3 = "{\"attributes\":[{\"name\":\"ref\",\"type\":\"REFERENCE\"},{\"name\":\"age\",\"type\":\"U8\"}],\"indexes\":[]}";
    private final String schema4 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"}],\"indexes\":[]}";

    private final ObjectMapper mapper = new ObjectMapper();

    public void testSchema1() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema1, SchemaDefinition.class);
        SchemaValidatorTransformer validator = new SchemaValidatorTransformer(
                def);

        List<ImmutableMap<String, Object>> instances = new ArrayList<ImmutableMap<String, Object>>();
        instances.add(ImmutableMap.<String, Object> of("isAwesome", true,
                "hotness", "COOL"));
        instances.add(ImmutableMap.<String, Object> of("isAwesome", true,
                "hotness", "COOL", "extra", 12345));

        for (ImmutableMap<String, Object> instance : instances) {
            validateInstance(validator, instance);
        }
    }

    public void testSchema2() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema2, SchemaDefinition.class);
        SchemaValidatorTransformer validator = new SchemaValidatorTransformer(
                def);

        List<ImmutableMap<String, Object>> instances = new ArrayList<ImmutableMap<String, Object>>();

        instances.add(ImmutableMap.<String, Object> of("when",
                new DateTime((System.currentTimeMillis() / 1000L) * 1000L,
                        DateTimeZone.UTC), "lname", "Dude"));
        instances.add(ImmutableMap.<String, Object> of("when", new DateTime(0,
                DateTimeZone.UTC), "lname", "Sweet"));

        for (ImmutableMap<String, Object> instance : instances) {
            validateInstance(validator, instance);
        }
    }

    public void testSchema3() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema3, SchemaDefinition.class);
        SchemaValidatorTransformer validator = new SchemaValidatorTransformer(
                def);

        List<ImmutableMap<String, Object>> instances = new ArrayList<ImmutableMap<String, Object>>();

        instances.add(ImmutableMap.<String, Object> of("ref", "foo:1", "age",
                18L));
        instances.add(ImmutableMap.<String, Object> of("ref", "bar:7", "age",
                21L));

        for (ImmutableMap<String, Object> instance : instances) {
            validateInstance(validator, instance);
        }
    }

    public void testSchema4() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema4, SchemaDefinition.class);
        SchemaValidatorTransformer validator = new SchemaValidatorTransformer(
                def);

        List<ImmutableMap<String, Object>> instances = new ArrayList<ImmutableMap<String, Object>>();

        instances.add(ImmutableMap.<String, Object> of("x", -1L, "y", 21L));
        instances.add(ImmutableMap.<String, Object> of("x", 10L, "y", -27L));

        for (ImmutableMap<String, Object> instance : instances) {
            validateInstance(validator, instance);
        }
    }

    private void validateInstance(SchemaValidatorTransformer validator,
            Map<String, Object> instance1) {
        Map<String, Object> transformed = validator
                .validateTransform(instance1);
        Map<String, Object> untransformed = validator.untransform(transformed);
        Assert.assertEquals(untransformed, instance1);
    }
}
