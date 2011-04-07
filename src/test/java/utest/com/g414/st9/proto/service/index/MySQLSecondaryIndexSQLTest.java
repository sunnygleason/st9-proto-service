package utest.com.g414.st9.proto.service.index;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.index.MySQLSecondaryIndex;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class MySQLSecondaryIndexSQLTest extends SecondaryIndexSQLTestBase {
    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema4, SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        MySQLSecondaryIndex mysql = new MySQLSecondaryIndex();

        Assert.assertEquals(
                "create table if not exists `_i_schema4__xy` (`_id` BIGINT UNSIGNED PRIMARY KEY, `_x` INT, `_y` INT)",
                mysql.getTableDefinition("schema4", "xy", def));

        Assert.assertEquals(
                "create index `_idx_schema4__xy` on `_i_schema4__xy` (`_x` ASC, `_y` ASC, `_id` ASC)",
                mysql.getIndexDefinition("schema4", "xy", def));
    }
}
