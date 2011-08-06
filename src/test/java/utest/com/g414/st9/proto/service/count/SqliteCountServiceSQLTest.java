package utest.com.g414.st9.proto.service.count;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.SqliteTypeHelper;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class SqliteCountServiceSQLTest extends CountServiceSQLTestBase {
    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        CountServiceTableHelper sqlite = new CountServiceTableHelper(
                SqliteTypeHelper.DATABASE_PREFIX, new SqliteTypeHelper());

        Assert.assertEquals(
                "create table if not exists `_c_schema4__01848a41d2c44a4b` (`_x` INT, `__hashcode` INT not null,`__count` INT, PRIMARY KEY(`_x`), UNIQUE(`__hashcode`))",
                sqlite.getTableDefinition("schema4", "xc", def));
    }
}
