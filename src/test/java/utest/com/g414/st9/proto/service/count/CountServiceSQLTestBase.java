package utest.com.g414.st9.proto.service.count;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.st9.proto.service.count.CountServiceTableHelper;
import com.g414.st9.proto.service.helper.MySQLTypeHelper;
import com.g414.st9.proto.service.helper.OpaquePaginationHelper;
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
                "create table if not exists `_c_schema4__01848a41d2c44a4b` (`_x` INT, `hashcode` BIGINT UNSIGNED not null,`count` BIGINT UNSIGNED, PRIMARY KEY(`_x`), UNIQUE(`hashcode`))",
                mysql.getTableDefinition("schema4", "xc", def));

        Assert.assertEquals(
                "insert ignore into `_c_schema4__01848a41d2c44a4b` (`_x`, `hashcode`, `count`) values (:x, :__hashcode, 0)",
                mysql.getInsertStatement("schema4", "xc", def,
                        ImmutableMap.<String, Object> of("x", 1)));

        Assert.assertEquals(
                "update `_c_schema4__01848a41d2c44a4b` set `count` = `count` + :__delta where `hashcode` = :__hashcode and `_x` = :x",
                mysql.getUpdateStatement("schema4", "xc", def,
                        ImmutableMap.<String, Object> of("x", 1)));

        Assert.assertEquals(
                "delete from `_c_schema4__01848a41d2c44a4b` where `hashcode` = :__hashcode and `_x` = :x and `count` = 0",
                mysql.getDeleteStatement("schema4", "xc", def,
                        ImmutableMap.<String, Object> of("x", 1)));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "x", new QueryValue(ValueType.INTEGER, "1")));

        Map<String, Object> bindParams0 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `count` from `_c_schema4__01848a41d2c44a4b` where `_x` = :p0 order by `_x` ASC limit 26 offset 0",
                mysql.getCounterQuery("schema4", "xc", query0, null,
                        OpaquePaginationHelper.DEFAULT_PAGE_SIZE, def,
                        bindParams0));

        Assert.assertEquals("{p0=1}", bindParams0.toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of();

        Map<String, Object> bindParams1 = new LinkedHashMap<String, Object>();

        Assert.assertEquals(
                "select `_x`, `count` from `_c_schema4__01848a41d2c44a4b` order by `_x` ASC limit 26 offset 0",
                mysql.getCounterQuery("schema4", "xc", query1, null,
                        OpaquePaginationHelper.DEFAULT_PAGE_SIZE, def,
                        bindParams1));

        Assert.assertEquals("{}", bindParams1.toString());
    }
}
