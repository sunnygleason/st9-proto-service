package utest.com.g414.st9.proto.service.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaValidatorTransformer;
import com.g414.st9.proto.service.validator.AnyValidator;
import com.g414.st9.proto.service.validator.BooleanValidator;
import com.g414.st9.proto.service.validator.EnumValidator;
import com.g414.st9.proto.service.validator.IntegerValidator;
import com.g414.st9.proto.service.validator.ReferenceValidator;
import com.g414.st9.proto.service.validator.StringValidator;
import com.g414.st9.proto.service.validator.UTCDateValidator;
import com.g414.st9.proto.service.validator.ValidationException;
import com.g414.st9.proto.service.validator.ValidatorTransformer;
import com.google.common.collect.ImmutableMap;

@Test
public class SchemaValidatorTest {
    private final String schema1 = "{\"attributes\":[{\"name\":\"isAwesome\",\"type\":\"BOOLEAN\"},{\"name\":\"hotness\",\"type\":\"ENUM\",\"values\":[\"FREEZING\",\"COLD\",\"COOL\",\"WARM\",\"TEH_HOTNESS\"]}],\"indexes\":[]}";
    private final String schema2 = "{\"attributes\":[{\"name\":\"when\",\"type\":\"UTC_DATE_SECS\"},{\"name\":\"lname\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"data\",\"type\":\"ANY\"}],\"indexes\":[]}";
    private final String schema3 = "{\"attributes\":[{\"name\":\"ref\",\"type\":\"REFERENCE\"},{\"name\":\"age\",\"type\":\"U8\"}],\"indexes\":[]}";
    private final String schema4 = "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"}],\"indexes\":[]}";
    private final DateTimeFormatter format = ISODateTimeFormat
            .basicDateTimeNoMillis();
    private final ObjectMapper mapper = new ObjectMapper();

    public void testSchema1() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema1, SchemaDefinition.class);
        SchemaValidatorTransformer validator = new SchemaValidatorTransformer(
                def);

        List<Map<String, Object>> instances = new ArrayList<Map<String, Object>>();
        instances.add(ImmutableMap.<String, Object> of("isAwesome", true,
                "hotness", "COOL"));
        instances.add(ImmutableMap.<String, Object> of("isAwesome", true,
                "hotness", "COOL", "extra", 12345));
        instances.add(mapper.readValue(
                "{\"isAwesome\":null,\"hotness\":\"COOL\"}",
                LinkedHashMap.class));
        instances.add(mapper.readValue(
                "{\"isAwesome\":false,\"hotness\":null}", LinkedHashMap.class));
        instances.add(mapper.readValue(
                "{\"isAwesome\":false,\"hotness\":\"COOL\",\"extra\":null}",
                LinkedHashMap.class));

        for (Map<String, Object> instance : instances) {
            validateInstance(validator, instance);
        }
    }

    public void testSchema2() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema2, SchemaDefinition.class);
        SchemaValidatorTransformer validator = new SchemaValidatorTransformer(
                def);

        List<ImmutableMap<String, Object>> instances = new ArrayList<ImmutableMap<String, Object>>();

        instances.add(ImmutableMap.<String, Object> of("when", format
                .print(new DateTime(
                        (System.currentTimeMillis() / 1000L) * 1000L,
                        DateTimeZone.UTC)), "lname", "Dude"));
        instances.add(ImmutableMap.<String, Object> of("when",
                format.print(new DateTime(0, DateTimeZone.UTC)), "lname",
                "Sweet"));
        instances.add(ImmutableMap.<String, Object> of("when",
                format.print(new DateTime(0, DateTimeZone.UTC)), "lname",
                "Nobody", "data", Long.valueOf(100)));
        instances.add(ImmutableMap.<String, Object> of("when",
                format.print(new DateTime(0, DateTimeZone.UTC)), "lname",
                "Different", "data", Collections.emptyMap()));

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

        instances.add(ImmutableMap.<String, Object> of("ref",
                "@foo:190272f987c6ac27", "age", 18L));
        instances.add(ImmutableMap.<String, Object> of("ref",
                "@bar:eff93f04beb16640", "age", 21L));

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

    public void testNulls() throws Exception {
        doValidateExpectException(new AnyValidator("ignored"));
        doValidateExpectException(new BooleanValidator("ignored"));
        doValidateExpectException(new EnumValidator("ignored",
                Collections.<String> emptyList()));
        doValidateExpectException(new IntegerValidator("ignored", "0", "1"));
        doValidateExpectException(new ReferenceValidator("ignored"));
        doValidateExpectException(new StringValidator("ignored", null, null));
        doValidateExpectException(new UTCDateValidator("ignored"));
    }

    private void doValidateExpectException(ValidatorTransformer<?, ?> v) {
        try {
            v.validateTransform(null);

            throw new RuntimeException("should have thrown!");
        } catch (ValidationException expected) {
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
