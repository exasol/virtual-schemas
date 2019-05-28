package com.exasol.adapter.sql;

import java.math.BigDecimal;
import java.util.Arrays;

import com.exasol.adapter.metadata.*;

public final class TestSqlStatementFactory {
    private static final String DUAL = "DUAL";
    private static final String SYSDUMMY = "SYSDUMMY1";

    static public SqlStatement createSelectOneFromSysDummy() {
        return selectOneFromTable(SYSDUMMY);
    }

    private static SqlStatement selectOneFromTable(final String tableName) {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("the_column")
                .type(DataType.createDecimal(18, 0)).build();
        final TableMetadata tableMetadata = new TableMetadata(tableName, "", Arrays.asList(columnMetadata), "");
        final SqlNode fromClause = new SqlTable(tableName, tableMetadata);
        final SqlSelectList selectList = SqlSelectList
                .createRegularSelectList(Arrays.asList(new SqlLiteralExactnumeric(BigDecimal.ONE)));
        return new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);
    }

    static public SqlStatement createSelectOneFromDual() {
        return selectOneFromTable(DUAL);
    }
}