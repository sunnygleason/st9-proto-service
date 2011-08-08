package utest.com.g414.st9.proto.service.count;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.helper.SqliteTypeHelper;
import com.g414.st9.proto.service.schema.AttributeType;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class SqliteCountServiceSQLTest extends CountServiceSQLTestBase {
    @Override
    public SqlTypeHelper getHelper() {
        return new SqliteTypeHelper();
    }

    @Override
    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SqlTypeHelper helper = getHelper();
        CountServiceTableHelper sqlite = new CountServiceTableHelper(
                helper.getPrefix(), helper);

        Assert.assertEquals(
                "create table if not exists `_c_schema4__01848a41d2c44a4b` (`_x` "
                        + helper.getSqlType(AttributeType.I32)
                        + ", `__hashcode` "
                        + helper.getSqlType(AttributeType.I64)
                        + " not null, `__count` "
                        + helper.getSqlType(AttributeType.U64)
                        + ", PRIMARY KEY(`_x`), UNIQUE(`__hashcode`))",
                sqlite.getTableDefinition("schema4", "xc", def));
    }
}
