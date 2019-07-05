package utils;

import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

import java.util.*;

import static com.exasol.adapter.sql.AggregateFunction.AVG;

/**
 * This class contains static methods for fast creation of SQL nodes which are used in tests for SQL generation
 * visitors. Helps to avoid duplication and speed up testing.
 */
public class SqlNodesCreator {
    private SqlNodesCreator() {
    }

    public static SqlOrderBy createSqlOrderByDescNullsFirst() {
        final List<SqlNode> orderByArguments = new ArrayList<>();
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2")
                .type(DataType.createDouble()).build();
        final List<Boolean> nulls = new ArrayList<>();
        nulls.add(false);
        nulls.add(true);
        orderByArguments.add(new SqlColumn(1, columnMetadata));
        orderByArguments.add(new SqlColumn(2, columnMetadata2));
        return new SqlOrderBy(orderByArguments, nulls, nulls);
    }

    public static SqlTable createFromClause(final List<ColumnMetadata> columns, final String tableName) {
        final TableMetadata tableMetadata = new TableMetadata("", "", columns, "");
        return new SqlTable(tableName, tableMetadata);
    }

    public static SqlSelectList createRegularSqlSelectListWithTwoColumns() {
        return SqlSelectList
                .createRegularSelectList(Arrays.asList(new SqlLiteralBool(true), new SqlLiteralString("string")));
    }

    public static SqlFunctionAggregate createSqlFunctionAggregate() {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(column);
        return new SqlFunctionAggregate(AVG, arguments, true);
    }

    public static SqlStatementSelect createSqlStatementSelect(final SqlSelectList sqlSelectList,
            final List<ColumnMetadata> columns, final String tableName) {
        final SqlTable fromClause = createFromClause(columns, tableName);
        return new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
    }

    public static SqlFunctionScalar createSqlFunctionScalarWithTwoStringArguments(ScalarFunction scalarFunction,
            String argument1, String argument2) {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString(argument1));
        arguments.add(new SqlLiteralString(argument2));
        return new SqlFunctionScalar(scalarFunction, arguments, true, false);
    }
}
