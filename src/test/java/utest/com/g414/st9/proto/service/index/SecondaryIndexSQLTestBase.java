package utest.com.g414.st9.proto.service.index;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

import utest.com.g414.st9.proto.service.schema.SchemaLoader;

import com.g414.st9.proto.service.SecondaryIndexResource;
import com.g414.st9.proto.service.helper.MySQLTypeHelper;
import com.g414.st9.proto.service.index.SecondaryIndexTableHelper;
import com.g414.st9.proto.service.query.QueryOperator;
import com.g414.st9.proto.service.query.QueryTerm;
import com.g414.st9.proto.service.query.QueryValue;
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
                "insert into `_i_schema4__0c6ca14baa3cd7d1` (`_x`, `_y`, `_id`) values (:x, :y, :id)",
                mysql.getInsertStatement("schema4", "xy", def));

        Assert.assertEquals(
                "update `_i_schema4__0c6ca14baa3cd7d1` set `_x` = :x, `_y` = :y where `_id` = :id",
                mysql.getUpdateStatement("schema4", "xy", def));

        Assert.assertEquals(
                "delete from `_i_schema4__0c6ca14baa3cd7d1` where `_id` = :id",
                mysql.getDeleteStatement("schema4", "xy"));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(
                new QueryTerm(QueryOperator.GT, "y", new QueryValue(
                        ValueType.INTEGER, "10")), new QueryTerm(
                        QueryOperator.GT, "x", new QueryValue(
                                ValueType.INTEGER, "1")), new QueryTerm(
                        QueryOperator.LT, "y", new QueryValue(
                                ValueType.INTEGER, "20")), new QueryTerm(
                        QueryOperator.EQ, "id", new QueryValue(
                                ValueType.INTEGER, "3")));

        Map<String, Object> bindParams0 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` > :p0 AND `_y` > :p1 AND `_y` < :p2 AND `_id` = :p3 order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query0, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def,
                        bindParams0));

        Assert.assertEquals("{p0=1, p1=10, p2=20, p3=3}",
                bindParams0.toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of(
                new QueryTerm(QueryOperator.GT, "y", new QueryValue(
                        ValueType.INTEGER, "10")), new QueryTerm(
                        QueryOperator.GT, "x", new QueryValue(
                                ValueType.INTEGER, "1")), new QueryTerm(
                        QueryOperator.LT, "y", new QueryValue(
                                ValueType.INTEGER, "20")));

        Map<String, Object> bindParams1 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` > :p0 AND `_y` > :p1 AND `_y` < :p2 order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query1, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def,
                        bindParams1));

        Assert.assertEquals("{p0=1, p1=10, p2=20}", bindParams1.toString());

        List<QueryTerm> query2 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.NE, "y", new QueryValue(ValueType.NULL, "")),
                new QueryTerm(QueryOperator.EQ, "x", new QueryValue(
                        ValueType.NULL, "")));

        Map<String, Object> bindParams2 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `_id` from `_i_schema4__0c6ca14baa3cd7d1` where `_x` is null AND `_y` is not null order by `_x` ASC, `_y` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema4", "xy", query2, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def,
                        bindParams2));

        Assert.assertEquals("{}", bindParams2.toString());
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
                "insert into `_i_schema5__57dbd25de1659c0f` (`_hotness`, `_id`) values (:hotness, :id)",
                mysql.getInsertStatement("schema5", "hotness", def));

        Assert.assertEquals(
                "update `_i_schema5__57dbd25de1659c0f` set `_hotness` = :hotness where `_id` = :id",
                mysql.getUpdateStatement("schema5", "hotness", def));

        Assert.assertEquals(
                "delete from `_i_schema5__57dbd25de1659c0f` where `_id` = :id",
                mysql.getDeleteStatement("schema5", "hotness"));

        List<QueryTerm> query0 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "hotness", new QueryValue(ValueType.STRING,
                        "\"hot\"")));

        Map<String, Object> bindParams0 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `_id` from `_i_schema5__57dbd25de1659c0f` where `_hotness` = :p0 order by `_hotness` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema5", "hotness", query0, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def,
                        bindParams0));

        Assert.assertEquals("{p0=0}", bindParams0.toString());

        List<QueryTerm> query1 = ImmutableList.<QueryTerm> of(new QueryTerm(
                QueryOperator.EQ, "hotness", new QueryValue(ValueType.STRING,
                        "\"cold\"")));

        Map<String, Object> bindParams1 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `_id` from `_i_schema5__57dbd25de1659c0f` where `_hotness` = :p0 order by `_hotness` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema5", "hotness", query1, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def,
                        bindParams1));

        Assert.assertEquals("{p0=1}", bindParams1.toString());

        List<QueryTerm> query2 = ImmutableList
                .<QueryTerm> of(new QueryTerm(QueryOperator.NE, "hotness",
                        new QueryValue(ValueType.NULL, "")));

        Map<String, Object> bindParams2 = new LinkedHashMap<String, Object>();
        Assert.assertEquals(
                "select `_id` from `_i_schema5__57dbd25de1659c0f` where `_hotness` is not null order by `_hotness` ASC, `_id` ASC limit 101 offset 0",
                mysql.getIndexQuery("schema5", "hotness", query2, null,
                        SecondaryIndexResource.DEFAULT_PAGE_SIZE, def,
                        bindParams2));

        Assert.assertEquals("{}", bindParams2.toString());

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
