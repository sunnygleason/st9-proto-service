package utest.com.g414.st9.proto.service.index;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.index.SqliteSecondaryIndex;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class SqliteSecondaryIndexSQLTest extends SecondaryIndexSQLTestBase {
    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper
                .readValue(schema4, SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SqliteSecondaryIndex sqlite = new SqliteSecondaryIndex();

        Assert.assertEquals(
                "create table if not exists `_i_schema4__xy` (`_id` INT PRIMARY KEY, `_x` INT, `_y` INT)",
                sqlite.getTableDefinition("schema4", "xy", def));

        Assert.assertEquals(
                "create index `_idx_schema4__xy` on `_i_schema4__xy` (`_x` ASC, `_y` ASC, `_id` ASC)",
                sqlite.getIndexDefinition("schema4", "xy", def));
    }
}
