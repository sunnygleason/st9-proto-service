package utest.com.g414.st9.proto.service.index;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.g414.st9.proto.service.helper.H2TypeHelper;
import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.index.SecondaryIndexTableHelper;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;

@Test
public class H2SecondaryIndexSQLTest extends SecondaryIndexSQLTestBase {
    @Override
    public SqlTypeHelper getHelper() {
        return new H2TypeHelper();
    }

    public void testSchemaSpecific() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SqlTypeHelper helper = getHelper();
        SecondaryIndexTableHelper h2 = new SecondaryIndexTableHelper(
                helper.getPrefix(), helper, getMockSequenceService());

        Assert.assertEquals(
                "create table if not exists \"_i_0001__0c6ca14baa3cd7d1\" (\"_id\" BIGINT UNSIGNED PRIMARY KEY, \"_x\" INT, \"_y\" INT, \"quarantined\" CHAR(1))",
                h2.getTableDefinition("schema4", "xy", def));

        Assert.assertEquals(
                "create index \"_idx_0001__0c6ca14baa3cd7d1\" on \"_i_0001__0c6ca14baa3cd7d1\" (\"_x\" ASC, \"_y\" ASC, \"_id\" ASC)",
                h2.getIndexDefinition("schema4", "xy", def));
    }
}
