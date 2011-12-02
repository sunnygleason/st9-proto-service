package itest.com.g414.st9.proto.service.count;

import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.count.CountServiceSQLTestBase;

import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.MySQLTypeHelper;
import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class MySQLCountServiceSQLTest extends CountServiceSQLTestBase {
    @Override
    public SqlTypeHelper getHelper() {
        return new MySQLTypeHelper();
    }

    @Override
    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        CountServiceTableHelper sqlite = new CountServiceTableHelper(
                MySQLTypeHelper.DATABASE_PREFIX, new MySQLTypeHelper(),
                getMockSequenceService());

        Assert.assertEquals(
                "create table if not exists `_c_0001__01848a41d2c44a4b` (`_x` INT, `__hashcode` BIGINT UNSIGNED not null, `__count` BIGINT UNSIGNED, PRIMARY KEY(`_x`), UNIQUE(`__hashcode`)) ENGINE=InnoDB ROW_FORMAT=DYNAMIC CHARACTER SET utf8",
                sqlite.getTableDefinition("schema4", "xc", def));
    }
}
