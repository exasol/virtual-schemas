package com.exasol.adapter.dialects.impl;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import com.google.common.collect.ImmutableList;

public class DialectTestData {

    public static SqlNode getTestSqlNode() {
        // SELECT USER_ID, count(URL) FROM CLICKS
        // WHERE 1 < USER_ID
        // GROUP BY USER_ID
        // HAVING 1 < COUNT(URL)
        // ORDER BY USER_ID
        // LIMIT 10;
        final TableMetadata clicksMeta = getClicksTableMetadata();
        final SqlTable fromClause = new SqlTable("CLICKS", clicksMeta);
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(ImmutableList
                .of(new SqlColumn(0, clicksMeta.getColumns().get(0)), new SqlFunctionAggregate(AggregateFunction.COUNT,
                        ImmutableList.<SqlNode>of(new SqlColumn(1, clicksMeta.getColumns().get(1))), false)));
        final SqlNode whereClause = new SqlPredicateLess(new SqlLiteralExactnumeric(BigDecimal.ONE),
                new SqlColumn(0, clicksMeta.getColumns().get(0)));
        final SqlExpressionList groupBy = new SqlGroupBy(
                ImmutableList.<SqlNode>of(new SqlColumn(0, clicksMeta.getColumns().get(0))));
        final SqlNode countUrl = new SqlFunctionAggregate(AggregateFunction.COUNT,
                ImmutableList.<SqlNode>of(new SqlColumn(1, clicksMeta.getColumns().get(1))), false);
        final SqlNode having = new SqlPredicateLess(new SqlLiteralExactnumeric(BigDecimal.ONE), countUrl);
        final SqlOrderBy orderBy = new SqlOrderBy(
                ImmutableList.<SqlNode>of(new SqlColumn(0, clicksMeta.getColumns().get(0))), ImmutableList.of(true),
                ImmutableList.of(true));
        final SqlLimit limit = new SqlLimit(10);
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

    public static TableMetadata getClicksTableMetadata() {
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("USER_ID", ColumnAdapterNotes.serialize(new ColumnAdapterNotes(3, "DECIMAL")),
                DataType.createDecimal(18, 0), true, false, "", ""));
        columns.add(new ColumnMetadata("URL", ColumnAdapterNotes.serialize(new ColumnAdapterNotes(12, "VARCHAR")),
                DataType.createVarChar(10000, DataType.ExaCharset.UTF8), true, false, "", ""));
        return new TableMetadata("CLICKS", "", columns, "");
    }

    public static SqlDialectContext getExasolDialectContext() {
        return new SqlDialectContext(new SchemaAdapterNotes(".", "\"", false, true, false, true, false, false, false,
                true, false, false, true, false));
    }

    public static SqlDialectContext getOracleDialectContext() {
        return new SqlDialectContext(new SchemaAdapterNotes(".", "\"", false, false, false, false, false, false, false,
                false, false, false, true, false));
    }
}
