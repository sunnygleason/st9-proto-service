package utest.com.g414.st9.proto.service.index;

import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.st9.proto.service.SecondaryIndexResource;
import com.g414.st9.proto.service.helper.MySQLTypeHelper;
import com.g414.st9.proto.service.helper.SqlParamBindings;
import com.g414.st9.proto.service.index.SecondaryIndexTableHelper;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
import com.g414.st9.proto.service.query.QueryValueList;
import com.g414.st9.proto.service.query.ValueType;
import com.g414.st9.proto.service.schema.SchemaDefinition;
import com.g414.st9.proto.service.schema.SchemaDefinitionValidator;
import com.google.inject.internal.ImmutableList;

@Test
public abstract class SecondaryIndexSQLTestBase {
    protected final String schema15 = SchemaLoader.loadSchema("schema15");
    protected final String schema16 = SchemaLoader.loadSchema("schema16");
    protected final String schema17 = SchemaLoader.loadSchema("schema17");

    protected final ObjectMapper mapper = new ObjectMapper();

    public abstract void testSchemaSpecific() throws Exception;

    public void testSchemaGeneric() throws Exception {
        SchemaDefinition def = mapper.readValue(schema15,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SecondaryIndexTableHelper mysql = new SecondaryIndexTableHelper(
                MySQLTypeHelper.DATABASE_PREFIX, new MySQLTypeHelper());

        Assert.assertEquals(
                "create table if not exists `_i_schema4__0c6ca14baa3cd7d1` (`_id` BIGINT UNSIGNED PRIMARY KEY, `_x` INT, `_y` INT)",
                mysql.getTableDefinition("schema4", "xy", def));

        Assert.assertEquals(
                "create index `_idx_schema4__0c6ca14baa3cd7d1` on `_i_schema4__0c6ca14baa3cd7d1` (`_x` ASC, `_y` ASC, `_id` ASC)",
                mysql.getIndexDefinition("schema4", "xy", def));

        Assert.assertEquals(
                "insert into `_i_schema4__0c6ca14baa3cd7d1` (`_x`, `_y`, `_id`) values (?, ?, ?)",
                mysql.getInsertStatement("schema4", "xy", def,
                        new SqlParamBindings(true)));

        Assert.assertEquals(
                "update `_i_schema4__0c6ca14baa3cd7d1` set `_x` = ?, `_y` = ? where `_id` = ?",
                mysql.getUpdateStatement("schema4", "xy", def,
                        new SqlParamBindings(true)));

        Assert.assertEquals(
                "delete from `_i_schema4__0c6ca14baa3cd7d1` where `_id` = ?",
                mysql.getDeleteStatement("schema4", "xy", new SqlParamBindings(
                        true)));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(
                new QueryTerm(QueryOperator.GT, "y", new QueryValue(
                        ValueType.INTEGER, "10")), new QueryTerm(
                        QueryOperator.GT, "x", new QueryValue(
                                ValueType.INTEGER, "1")), new QueryTerm(
                        QueryOperator.LT, "y", new QueryValue(
                                ValueType.INTEGER, "20")), new QueryTerm(
                        QueryOperator.EQ, "id", new QueryValue(
                                ValueType.STRING, "\"foo:3\"")));

        SqlParamBindings bind0 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` > ? AND `_y` > ? AND `_y` < ? AND `_id` = ? order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query0, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind0));

        Assert.assertEquals("{p0=1, p1=10, p2=20, p3=3}", bind0.asMap()
                .toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of(
                new QueryTerm(QueryOperator.GT, "y", new QueryValue(
                        ValueType.INTEGER, "10")), new QueryTerm(
                        QueryOperator.GT, "x", new QueryValue(
                                ValueType.INTEGER, "1")), new QueryTerm(
                        QueryOperator.LT, "y", new QueryValue(
                                ValueType.INTEGER, "20")));

        SqlParamBindings bind1 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` > ? AND `_y` > ? AND `_y` < ? order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query1, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind1));

        Assert.assertEquals("{p0=1, p1=10, p2=20}", bind1.asMap().toString());

        List<QueryTerm> query2 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.NE, "y", new QueryValue(ValueType.NULL, "")),
                new QueryTerm(QueryOperator.EQ, "x", new QueryValue(
                        ValueType.NULL, "")));

        SqlParamBindings bind2 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` is null AND `_y` is not null order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query2, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind2));

        Assert.assertEquals("{}", bind2.asMap().toString());

        List<QueryTerm> query3 = ImmutableList.<QueryTerm> of(
                new QueryTerm(QueryOperator.IN, "x", new QueryValueList(
                        ImmutableList.of(
                                new QueryValue(ValueType.INTEGER, "1"),
                                new QueryValue(ValueType.INTEGER, "2"),
                                new QueryValue(ValueType.INTEGER, "3")))),
                new QueryTerm(QueryOperator.LT, "y", new QueryValue(
                        ValueType.INTEGER, "0")));

        SqlParamBindings bind3 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` in( ?,  ?,  ?) AND `_y` < ? order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query3, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind3));

        Assert.assertEquals("{p0=1, p1=2, p2=3, p3=0}", bind3.asMap()
                .toString());

    }

    public void testSchemaSpecialFeatures() throws Exception {
        SchemaDefinition def = mapper.readValue(schema16,
                SchemaDefinition.class);

        SchemaDefinitionValidator v = new SchemaDefinitionValidator();
        v.validate(def);

        SecondaryIndexTableHelper mysql = new SecondaryIndexTableHelper(
                MySQLTypeHelper.DATABASE_PREFIX, new MySQLTypeHelper());

        Assert.assertEquals(
                "create table if not exists `_i_schema5__57dbd25de1659c0f` (`_id` BIGINT UNSIGNED PRIMARY KEY, `_hotness` SMALLINT)",
                mysql.getTableDefinition("schema5", "hotness", def));

        Assert.assertEquals(
                "create index `_idx_schema5__57dbd25de1659c0f` on `_i_schema5__57dbd25de1659c0f` (`_hotness` ASC, `_id` ASC)",
                mysql.getIndexDefinition("schema5", "hotness", def));

        Assert.assertEquals(
                "insert into `_i_schema5__57dbd25de1659c0f` (`_hotness`, `_id`) values (?, ?)",
                mysql.getInsertStatement("schema5", "hotness", def,
                        new SqlParamBindings(true)));

        Assert.assertEquals(
                "update `_i_schema5__57dbd25de1659c0f` set `_hotness` = ? where `_id` = ?",
                mysql.getUpdateStatement("schema5", "hotness", def,
                        new SqlParamBindings(true)));

        Assert.assertEquals(
                "delete from `_i_schema5__57dbd25de1659c0f` where `_id` = ?",
                mysql.getDeleteStatement("schema5", "hotness",
                        new SqlParamBindings(true)));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "hotness", new QueryValue(ValueType.STRING,
                        "\"hot\"")));

        SqlParamBindings bind0 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema5__57dbd25de1659c0f` where `_hotness` = ? order by `_hotness` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema5", "hotness", query0, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind0));

        Assert.assertEquals("{p0=0}", bind0.asMap().toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "hotness", new QueryValue(ValueType.STRING,
                        "\"cold\"")));

        SqlParamBindings bind1 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema5__57dbd25de1659c0f` where `_hotness` = ? order by `_hotness` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema5", "hotness", query1, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind1));

        Assert.assertEquals("{p0=1}", bind1.asMap().toString());

        List<QueryTerm> query2 = ImmutableList
                .<QueryTerm> of(new QueryTerm(QueryOperator.NE, "hotness",
                        new QueryValue(ValueType.NULL, "")));

        SqlParamBindings bind2 = new SqlParamBindings(true);
        Assert.assertEquals(
                "select `_id` from `_i_schema5__57dbd25de1659c0f` where `_hotness` is not null order by `_hotness` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema5", "hotness", query2, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def, bind2));

        Assert.assertEquals("{}", bind2.asMap().toString());

        SchemaDefinition def2 = mapper.readValue(schema17,
                SchemaDefinition.class);

        SchemaDefinitionValidator v2 = new SchemaDefinitionValidator();
        v2.validate(def2);

        Assert.assertEquals(
                "create table if not exists `_i_schema6__57dbd25de1659c0f` (`_id` BIGINT UNSIGNED PRIMARY KEY, `_hotness` SMALLINT)",
                mysql.getTableDefinition("schema6", "hotness", def2));

        Assert.assertEquals(
                "create unique index `_idx_schema6__57dbd25de1659c0f` on `_i_schema6__57dbd25de1659c0f` (`_hotness` ASC)",
                mysql.getIndexDefinition("schema6", "hotness", def2));

    }
}
