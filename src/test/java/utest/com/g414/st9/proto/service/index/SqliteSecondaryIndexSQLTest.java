package utest.com.g414.st9.proto.service.index;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.helper.SqliteTypeHelper;
import com.g414.st9.proto.service.index.SecondaryIndexTableHelper;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class SqliteSecondaryIndexSQLTest extends SecondaryIndexSQLTestBase {
    @Override
    public SqlTypeHelper getHelper() {
        return new SqliteTypeHelper();
    }

    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SqlTypeHelper helper = getHelper();
        SecondaryIndexTableHelper sqlite = new SecondaryIndexTableHelper(
                helper.getPrefix(), helper);

        Assert.assertEquals(
                "create table if not exists `_i_schema4__0c6ca14baa3cd7d1` (`_id` INT PRIMARY KEY, `_x` INT, `_y` INT)",
                sqlite.getTableDefinition("schema4", "xy", def));

        Assert.assertEquals(
                "create index `_idx_schema4__0c6ca14baa3cd7d1` on `_i_schema4__0c6ca14baa3cd7d1` (`_x` ASC, `_y` ASC, `_id` ASC)",
                sqlite.getIndexDefinition("schema4", "xy", def));
    }
}
