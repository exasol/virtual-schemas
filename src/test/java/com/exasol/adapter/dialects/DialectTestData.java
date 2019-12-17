package com.exasol.adapter.dialects;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.adapternotes.ColumnAdapterNotes;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

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
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(
                List.of(new SqlColumn(0, clicksMeta.getColumns().get(0)), new SqlFunctionAggregate(
                        AggregateFunction.COUNT, List.of(new SqlColumn(1, clicksMeta.getColumns().get(1))), false)));
        final SqlNode whereClause = new SqlPredicateLess(new SqlLiteralExactnumeric(BigDecimal.ONE),
                new SqlColumn(0, clicksMeta.getColumns().get(0)));
        final SqlExpressionList groupBy = new SqlGroupBy(List.of(new SqlColumn(0, clicksMeta.getColumns().get(0))));
        final SqlNode countUrl = new SqlFunctionAggregate(AggregateFunction.COUNT,
                List.of(new SqlColumn(1, clicksMeta.getColumns().get(1))), false);
        final SqlNode having = new SqlPredicateLess(new SqlLiteralExactnumeric(BigDecimal.ONE), countUrl);
        final SqlOrderBy orderBy = new SqlOrderBy(List.of(new SqlColumn(0, clicksMeta.getColumns().get(0))),
                List.of(true), List.of(true));
        final SqlLimit limit = new SqlLimit(10);
        return SqlStatementSelect.builder().selectList(selectList).fromClause(fromClause).whereClause(whereClause)
                .groupBy(groupBy).having(having).orderBy(orderBy).limit(limit).build();
    }

    public static TableMetadata getClicksTableMetadata() {
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("USER_ID")
                .adapterNotes(ColumnAdapterNotes.serialize(new ColumnAdapterNotes(3, "DECIMAL")))
                .type(DataType.createDecimal(18, 0)).nullable(true).identity(false).defaultValue("").comment("")
                .build());
        columns.add(ColumnMetadata.builder().name("URL")
                .adapterNotes(ColumnAdapterNotes.serialize(new ColumnAdapterNotes(12, "VARCHAR")))
                .type(DataType.createVarChar(10000, DataType.ExaCharset.UTF8)).nullable(true).identity(false)
                .defaultValue("").comment("").build());
        return new TableMetadata("CLICKS", "", columns, "");
    }
}