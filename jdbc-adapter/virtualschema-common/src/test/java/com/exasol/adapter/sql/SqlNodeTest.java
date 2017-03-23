package com.exasol.adapter.sql;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.utils.SqlTestUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlNodeTest {
    
    @Test
    public void testToSimpleSql() throws MetadataException {
        SqlNode node = getTestSqlNode();
        String expectedSql = "SELECT \"USER_ID\", COUNT(\"URL\") FROM \"CLICKS\"" +
        " WHERE 1 < \"USER_ID\"" +
        " GROUP BY \"USER_ID\"" +
        " HAVING 1 < COUNT(\"URL\")" +
        " ORDER BY \"USER_ID\"" +
        " LIMIT 10";
        String actualSql = node.toSimpleSql();
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    private SqlNode getTestSqlNode() throws MetadataException {
        // SELECT USER_ID, count(URL) FROM CLICKS
        // WHERE 1 < USER_ID
        // GROUP BY USER_ID
        // HAVING 1 < COUNT(URL)
        // ORDER BY USER_ID
        // LIMIT 10;
        TableMetadata clicksMeta = getClicksTableMetadata();
        SqlTable fromClause = new SqlTable("CLICKS", clicksMeta);
        SqlSelectList selectList = SqlSelectList.createRegularSelectList(ImmutableList.of(
                new SqlColumn(0, clicksMeta.getColumns().get(0)),
                new SqlFunctionAggregate(AggregateFunction.COUNT, ImmutableList.<SqlNode>of(new SqlColumn(1, clicksMeta.getColumns().get(1))), false)));
        SqlNode whereClause = new SqlPredicateLess(new SqlLiteralExactnumeric(BigDecimal.ONE), new SqlColumn(0, clicksMeta.getColumns().get(0)));
        SqlExpressionList groupBy = new SqlGroupBy(ImmutableList.<SqlNode>of(new SqlColumn(0, clicksMeta.getColumns().get(0))));
        SqlNode countUrl = new SqlFunctionAggregate(AggregateFunction.COUNT, ImmutableList.<SqlNode>of(new SqlColumn(1, clicksMeta.getColumns().get(1))), false);
        SqlNode having = new SqlPredicateLess(new SqlLiteralExactnumeric(BigDecimal.ONE), countUrl);
        SqlOrderBy orderBy = new SqlOrderBy(ImmutableList.<SqlNode>of(new SqlColumn(0, clicksMeta.getColumns().get(0))), ImmutableList.of(true), ImmutableList.of(true));
        SqlLimit limit = new SqlLimit(10);
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }
    
    private TableMetadata getClicksTableMetadata() throws MetadataException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("USER_ID", "", DataType.createDecimal(18, 0), true, false, "", ""));
        columns.add(new ColumnMetadata("URL", "", DataType.createVarChar(10000, ExaCharset.UTF8), true, false, "", ""));
        return new TableMetadata("CLICKS", "", columns, "");
    }

}
