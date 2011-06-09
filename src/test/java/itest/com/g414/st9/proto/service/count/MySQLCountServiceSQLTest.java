package itest.com.g414.st9.proto.service.count;

import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.count.CountServiceSQLTestBase;

import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.MySQLTypeHelper;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class MySQLCountServiceSQLTest extends CountServiceSQLTestBase {
    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        CountServiceTableHelper sqlite = new CountServiceTableHelper(
                MySQLTypeHelper.DATABASE_PREFIX, new MySQLTypeHelper());

        Assert.assertEquals(
                "create table if not exists `_c_schema4__xc` (`_x` INT, `hashcode` BIGINT UNSIGNED not null,`count` BIGINT UNSIGNED, PRIMARY KEY(`_x`), UNIQUE(`hashcode`))",
                sqlite.getTableDefinition("schema4", "xc", def));
    }
}
