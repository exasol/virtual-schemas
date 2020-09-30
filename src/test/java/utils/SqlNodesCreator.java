package utils;

import static com.exasol.adapter.sql.AggregateFunction.AVG;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;

/**
 * This class contains static methods for fast creation of SQL nodes which are used in tests for SQL generation
 * visitors. Helps to avoid duplication and speed up testing.
 */
public class SqlNodesCreator {
    private SqlNodesCreator() {
    }

    public static SqlOrderBy createSqlOrderByDescNullsFirst(final String columnName1, final String columnName2) {
        final List<SqlNode> orderByArguments = new ArrayList<>();
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2")
                .type(DataType.createDouble()).build();
        orderByArguments.add(new SqlColumn(1, columnMetadata));
        orderByArguments.add(new SqlColumn(2, columnMetadata2));
        return new SqlOrderBy(orderByArguments, Stream.of(false, true).collect(Collectors.toList()),
                Stream.of(false, true).collect(Collectors.toList()));
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
        return SqlStatementSelect.builder().selectList(sqlSelectList).fromClause(fromClause).build();
    }

    public static SqlFunctionScalar createSqlFunctionScalarWithTwoStringArguments(final ScalarFunction scalarFunction,
            final String argument1, final String argument2) {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString(argument1));
        arguments.add(new SqlLiteralString(argument2));
        return new SqlFunctionScalar(scalarFunction, arguments);
    }

    public static SqlFunctionScalar createSqlFunctionScalarForDateTest(final ScalarFunction scalarFunction,
            final int numericValue) {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column")
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}")
                        .type(DataType.createChar(20, DataType.ExaCharset.UTF8)).build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(numericValue)));
        return new SqlFunctionScalar(scalarFunction, arguments);
    }

    public static SqlSelectList createSqlSelectStarListWithOneColumn(final String adapterNotes, final DataType dataType,
            final String columnName) {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name(columnName).adapterNotes(adapterNotes).type(dataType).build());
        final SqlNode sqlStatementSelect = createSqlStatementSelect(selectList, columns, "");
        selectList.setParent(sqlStatementSelect);
        return selectList;
    }

    public static SqlSelectList createSqlSelectStarListWithoutColumns() {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final SqlNode sqlStatementSelect = createSqlStatementSelect(sqlSelectList, Collections.emptyList(),
                "test_table");
        sqlSelectList.setParent(sqlStatementSelect);
        return sqlSelectList;
    }
}