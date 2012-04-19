package utest.com.g414.st9.proto.service.schema;

import java.util.Arrays;
import java.util.List;

import org.testng.annotations.Test;

import com.g414.st9.proto.service.schema.Attribute;
import com.g414.st9.proto.service.schema.AttributeType;
import com.g414.st9.proto.service.schema.SchemaValidationHelper;
import com.g414.st9.proto.service.validator.ValidationException;

@Test
public class SchemaValidationHelperTest {
    public void testAttributeUpgrades() {
        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.ENUM, "dude"),
                a("foo", AttributeType.ENUM, "dude"));

        for (AttributeType type : new AttributeType[] { AttributeType.ANY,
                AttributeType.ARRAY, AttributeType.BOOLEAN,
                AttributeType.CHAR_ONE, AttributeType.MAP }) {
            SchemaValidationHelper.validateAttributeUpgrade(a("foo", type),
                    a("foo", type));
            SchemaValidationHelper.validateAttributeUpgrade(a("foo", type),
                    null);
        }

        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.REFERENCE),
                a("foo", AttributeType.REFERENCE));

        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTC_DATE_SECS),
                a("foo", AttributeType.UTC_DATE_SECS));
        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTC_DATE_SECS),
                a("foo", AttributeType.I64));
        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTC_DATE_SECS), null);

        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTF8_SMALLSTRING),
                a("foo", AttributeType.UTF8_SMALLSTRING));
        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTF8_SMALLSTRING),
                a("foo", AttributeType.UTF8_TEXT));
        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTF8_SMALLSTRING), null);

        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTF8_TEXT),
                a("foo", AttributeType.UTF8_TEXT));
        SchemaValidationHelper.validateAttributeUpgrade(
                a("foo", AttributeType.UTF8_SMALLSTRING), null);
    }

    public void testNumericUpgrades() {
        validateNumericType(new AttributeType[] { AttributeType.I8,
                AttributeType.I16, AttributeType.I32, AttributeType.I64 });
        validateNumericType(new AttributeType[] { AttributeType.I16,
                AttributeType.I32, AttributeType.I64 });
        validateNumericType(new AttributeType[] { AttributeType.I32,
                AttributeType.I64 });
        validateNumericType(new AttributeType[] { AttributeType.I64 });

        validateNumericType(new AttributeType[] { AttributeType.U8,
                AttributeType.U16, AttributeType.U32, AttributeType.U64 });
        validateNumericType(new AttributeType[] { AttributeType.U16,
                AttributeType.U32, AttributeType.U64 });
        validateNumericType(new AttributeType[] { AttributeType.U32,
                AttributeType.U64 });
        validateNumericType(new AttributeType[] { AttributeType.U64 });
    }

    public void testEnumUpgrades() {
        SchemaValidationHelper.assertEnumAttributeCompatible(
                a("foo", AttributeType.ENUM, "dude"),
                a("foo", AttributeType.ENUM, "dude"));
        SchemaValidationHelper.assertEnumAttributeCompatible(
                a("foo", AttributeType.ENUM, "dude"),
                a("foo", AttributeType.ENUM, "dude", "dude2"));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testIllegalEnumUpgradeFewer() {
        SchemaValidationHelper.assertEnumAttributeCompatible(
                a("foo", AttributeType.ENUM, "dude"),
                a("foo", AttributeType.ENUM));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testIllegalEnumUpgradeReorder() {
        SchemaValidationHelper.assertEnumAttributeCompatible(
                a("foo", AttributeType.ENUM, "dude", "dude2"),
                a("foo", AttributeType.ENUM, "dude2", "dude"));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testIllegalEnumUpgradeInsertAtFront() {
        SchemaValidationHelper.assertEnumAttributeCompatible(
                a("foo", AttributeType.ENUM, "dude", "dude2"),
                a("foo", AttributeType.ENUM, "dude0", "dude", "dude2"));
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testIllegalEnumUpgradeInsertInMiddle() {
        SchemaValidationHelper.assertEnumAttributeCompatible(
                a("foo", AttributeType.ENUM, "dude", "dude2", "dude3"),
                a("foo", AttributeType.ENUM, "dude", "dude2", "dude2.5",
                        "dude3"));
    }

    private static Attribute a(String name, AttributeType type,
            String... values) {
        List<String> valueList = values == null ? null : Arrays.asList(values);

        return new Attribute(name, type, null, null, valueList, null);
    }

    private void validateNumericType(AttributeType[] possibleTypes) {
        AttributeType baseType = possibleTypes[0];
        for (AttributeType type : possibleTypes) {
            SchemaValidationHelper.validateAttributeUpgrade(a("foo", baseType),
                    a("foo", type));
        }
        SchemaValidationHelper.validateAttributeUpgrade(a("foo", baseType),
                null);
    }

}
