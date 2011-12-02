package utest.com.g414.st9.proto.service.count;

import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.st9.proto.service.CounterResource;
import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.SqlParamBindings;
import com.g414.st9.proto.service.helper.SqlTypeHelper;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.AttributeType;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;
import com.g414.st9.proto.service.sequence.SequenceService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Test
public abstract class CountServiceSQLTestBase {
    protected final String schema15 = SchemaLoader.loadSchema("schema18");
    protected final ObjectMapper mapper = new ObjectMapper();

    public abstract void testSchemaSpecific() throws Exception;

    public abstract SqlTypeHelper getHelper();

    public SequenceService getMockSequenceService() {
        return new SequenceService() {
            @Override
            public String getTypeName(Integer id) throws Exception {
                return "mock_type";
            }

            @Override
            public Integer getTypeId(String type, boolean create)
                    throws Exception {
                return 1;
            }

            @Override
            public Integer getTypeId(String type, boolean create, boolean strict)
                    throws Exception {
                return 1;
            }
        };
    }

    public void testSchemaGeneric() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SqlTypeHelper helper = getHelper();
        CountServiceTableHelper mysql = new CountServiceTableHelper(
                helper.getPrefix(), helper, getMockSequenceService());

        Assert.assertEquals(
                "create table if not exists "
                        + helper.quote("_c_0001__01848a41d2c44a4b") + " ("
                        + helper.quote("_x") + " "
                        + helper.getSqlType(AttributeType.I32) + ", "
                        + helper.quote("__hashcode") + " "
                        + helper.getSqlType(AttributeType.U64) + " not null, "
                        + helper.quote("__count") + " "
                        + helper.getSqlType(AttributeType.U64)
                        + ", PRIMARY KEY(" + helper.quote("_x") + "), UNIQUE("
                        + helper.quote("__hashcode") + "))"
                        + helper.getTableOptions(),
                mysql.getTableDefinition("schema4", "xc", def));

        Assert.assertEquals(
                helper.getInsertIgnore() + " into "
                        + helper.quote("_c_0001__01848a41d2c44a4b") + " ("
                        + helper.quote("_x") + ", "
                        + helper.quote("__hashcode") + ", "
                        + helper.quote("__count") + ") values (?, ?, 0)", mysql
                        .getInsertStatement("schema4", "xc", def,
                                ImmutableMap.<String, Object> of("x", 1),
                                new SqlParamBindings(true)));

        Assert.assertEquals(
                "update " + helper.quote("_c_0001__01848a41d2c44a4b") + " set "
                        + helper.quote("__count") + " = "
                        + helper.quote("__count") + " + ? where "
                        + helper.quote("__hashcode") + " = ? and "
                        + helper.quote("_x") + " = ?", mysql
                        .getUpdateStatement("schema4", "xc", def,
                                ImmutableMap.<String, Object> of("x", 1),
                                new SqlParamBindings(true)));

        Assert.assertEquals(
                "delete from " + helper.quote("_c_0001__01848a41d2c44a4b")
                        + " where " + helper.quote("__count") + " = 0 and "
                        + helper.quote("__hashcode") + " = ? and "
                        + helper.quote("_x") + " = ?", mysql
                        .getDeleteStatement("schema4", "xc", def,
                                ImmutableMap.<String, Object> of("x", 1),
                                new SqlParamBindings(true)));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "x", new QueryValue(ValueType.INTEGER, "1")));

        SqlParamBindings bind0 = new SqlParamBindings(true);
        Assert.assertEquals("select " + helper.quote("__count") + " from "
                + helper.quote("_c_0001__01848a41d2c44a4b") + " where "
                + helper.quote("_x") + " = ? order by " + helper.quote("_x")
                + " ASC limit 1001 offset 0", mysql.getCounterQuery("schema4",
                "xc", query0, null, CounterResource.DEFAULT_PAGE_SIZE, def,
                bind0));

        Assert.assertEquals("{p0=1}", bind0.asMap().toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of();

        SqlParamBindings bind1 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select " + helper.quote("_x") + ", " + helper.quote("__count")
                        + " from " + helper.quote("_c_0001__01848a41d2c44a4b")
                        + " order by " + helper.quote("_x")
                        + " ASC limit 1001 offset 0", mysql.getCounterQuery(
                        "schema4", "xc", query1, null,
                        CounterResource.DEFAULT_PAGE_SIZE, def, bind1));

        Assert.assertEquals("{}", bind1.asMap().toString());
    }
}
