package utest.com.g414.st9.proto.service.count;

import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.st9.proto.service.CounterResource;
import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.MySQLTypeHelper;
import com.g414.st9.proto.service.helper.SqlParamBindings;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Test
public abstract class CountServiceSQLTestBase {
    protected final String schema15 = SchemaLoader.loadSchema("schema18");
    protected final ObjectMapper mapper = new ObjectMapper();

    public abstract void testSchemaSpecific() throws Exception;

    public void testSchemaGeneric() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        CountServiceTableHelper mysql = new CountServiceTableHelper(
                MySQLTypeHelper.DATABASE_PREFIX, new MySQLTypeHelper());

        Assert.assertEquals(
                "create table if not exists `_c_schema4__01848a41d2c44a4b` (`_x` INT, `__hashcode` BIGINT UNSIGNED not null,`__count` BIGINT UNSIGNED, PRIMARY KEY(`_x`), UNIQUE(`__hashcode`))",
                mysql.getTableDefinition("schema4", "xc", def));

        Assert.assertEquals(
                "insert ignore into `_c_schema4__01848a41d2c44a4b` (`_x`, `__hashcode`, `__count`) values (?, ?, 0)",
                mysql.getInsertStatement("schema4", "xc", def,
                        ImmutableMap.<String, Object> of("x", 1),
                        new SqlParamBindings(true)));

        Assert.assertEquals(
                "update `_c_schema4__01848a41d2c44a4b` set `__count` = `__count` + ? where `__hashcode` = ? and `_x` = ?",
                mysql.getUpdateStatement("schema4", "xc", def,
                        ImmutableMap.<String, Object> of("x", 1),
                        new SqlParamBindings(true)));

        Assert.assertEquals(
                "delete from `_c_schema4__01848a41d2c44a4b` where `__count` = 0 and `__hashcode` = ? and `_x` = ?",
                mysql.getDeleteStatement("schema4", "xc", def,
                        ImmutableMap.<String, Object> of("x", 1),
                        new SqlParamBindings(true)));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "x", new QueryValue(ValueType.INTEGER, "1")));

        SqlParamBindings bind0 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `__count` from `_c_schema4__01848a41d2c44a4b` where `_x` = ? order by `_x` ASC limit 1001 offset 0",
                mysql.getCounterQuery("schema4", "xc", query0, null,
                        CounterResource.DEFAULT_PAGE_SIZE, def, bind0));

        Assert.assertEquals("{p0=1}", bind0.asMap().toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of();

        SqlParamBindings bind1 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_x`, `__count` from `_c_schema4__01848a41d2c44a4b` order by `_x` ASC limit 1001 offset 0",
                mysql.getCounterQuery("schema4", "xc", query1, null,
                        CounterResource.DEFAULT_PAGE_SIZE, def, bind1));

        Assert.assertEquals("{}", bind1.asMap().toString());
    }
}
