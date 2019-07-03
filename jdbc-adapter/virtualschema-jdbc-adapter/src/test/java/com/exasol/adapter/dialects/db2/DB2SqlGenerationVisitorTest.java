package com.exasol.adapter.dialects.db2;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.mockito.*;

import java.math.*;
import java.sql.*;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DB2SqlGenerationVisitorTest {
    private SqlGenerationVisitor visitor;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new DB2SqlDialect(this.connectionMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new DB2SqlGenerationVisitor(dialect, context);
    }

    @Test
    void visitSqlColumnWithoutParent() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        assertThat(visitor.visit(column), equalTo("\"test_column\""));
    }

    @Test
    void visitSqlColumnWithParentXml() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"XML\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column),
                equalTo("XMLSERIALIZE(\"test_column\" as VARCHAR(32000) INCLUDING XMLDECLARATION)"));
    }

    @Test
    void visitSqlColumnWithParentClob() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":2005, \"typeName\":\"CLOB\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column), equalTo("CAST(SUBSTRING(\"test_column\",32672) AS VARCHAR(32672))"));
    }

    @Test
    void visitSqlColumnWithParentChar() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":1, \"typeName\":\"CHAR () FOR BIT DATA\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column), equalTo("HEX(\"test_column\")"));
    }

    @Test
    void visitSqlColumnWithParentVarchar() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":12, \"typeName\":\"VARCHAR () FOR BIT DATA\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column), equalTo("HEX(\"test_column\")"));
    }

    @Test
    void visitSqlColumnWithParentTime() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":92, \"typeName\":\"TIME\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column), equalTo("VARCHAR(\"test_column\")"));
    }

    @Test
    void visitSqlColumnWithParentTimestamp() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":93, \"typeName\":\"TIMESTAMP\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column), equalTo("VARCHAR(\"test_column\")"));
    }

    @Test
    void visitSqlColumnWithParentTypeNameIsNotSupported() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":2004, \"typeName\":\"BLOB\"}").build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(visitor.visit(column), equalTo("'BLOB NOT SUPPORTED'"));
    }

    @Test
    void visitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(visitor.visit(select), //
                equalTo("SELECT \"USER_ID\", " //
                        + "COUNT(\"URL\") FROM \"test_schema\".\"CLICKS\" " //
                        + "WHERE 1 < \"USER_ID\" " //
                        + "GROUP BY \"USER_ID\" " //
                        + "HAVING 1 < COUNT(\"URL\") " //
                        + "ORDER BY \"USER_ID\" FETCH FIRST 10 ROWS ONLY"));
    }

    @Test
    void visitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(visitor.visit(sqlSelectList), equalTo("1"));
    }

    @Test
    void visitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final TableMetadata tableMetadata = new TableMetadata("test_table", "", Collections.EMPTY_LIST, "");
        final SqlTable fromClause = new SqlTable("test_table_name", tableMetadata);
        final SqlNode select = new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
        sqlSelectList.setParent(select);
        assertThat(visitor.visit(sqlSelectList), equalTo("*"));
    }

    @Test
    void visitSqlSelectListSelectStarRequiresCast() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"XML\"}")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final TableMetadata tableMetadata = new TableMetadata("test_table", "", columns, "");
        final SqlTable fromClause = new SqlTable("test_table_name", tableMetadata);
        final SqlNode select = new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
        sqlSelectList.setParent(select);
        assertThat(visitor.visit(sqlSelectList),
                equalTo("XMLSERIALIZE(\"test_column\" as VARCHAR(32000) INCLUDING XMLDECLARATION)"));
    }

    @Test
    void visitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final TableMetadata tableMetadata = new TableMetadata("test_table", "", columns, "");
        final SqlTable fromClause = new SqlTable("test_table_name", tableMetadata);
        final SqlNode select = new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
        sqlSelectList.setParent(select);
        assertThrows(SqlGenerationVisitorException.class, () -> visitor.visit(sqlSelectList));
    }

    @Test
    void visitSqlFunctionScalarTrimOneArgument() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.TRIM, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("TRIM('test')"));
    }

    @Test
    void visitSqlFunctionScalarTrimOTwoArguments() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        arguments.add(new SqlLiteralString("test2"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.TRIM, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("TRIM('test2' FROM 'test')"));
    }

    @Test
    void visitSqlFunctionScalarAddDays() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ADD_DAYS, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + 10 DAYS)"));
    }

    @Test
    void visitSqlFunctionScalarAddHours() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ADD_HOURS, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + 10 HOURS)"));
    }

    @Test
    void visitSqlFunctionScalarAddMinutes() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ADD_MINUTES, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + 10 MINUTES)"));
    }

    @Test
    void visitSqlFunctionScalarAddSeconds() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ADD_SECONDS, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + 10 SECONDS)"));
    }

    @Test
    void visitSqlFunctionScalarAddYears() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ADD_YEARS, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + 10 YEARS)"));
    }

    @Test
    void visitSqlFunctionScalarAddWeeks() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ADD_WEEKS, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + 10 WEEKS)"));
    }

    @CsvSource({ "SYSDATE", "CURRENT_DATE" })
    @ParameterizedTest
    void visitSqlFunctionScalarCurrentDate(final ScalarFunction scalarFunction) throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CURRENT DATE"));
    }

    @Test
    void visitSqlFunctionScalarCurrentTimestamp() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.DBTIMEZONE, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("DBTIMEZONE"));
    }

    @Test
    void visitSqlFunctionScalarLocalTimestamp() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.LOCALTIMESTAMP, null, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("LOCALTIMESTAMP"));
    }

    @Test
    void visitSqlFunctionScalarSessionTimezone() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.SESSIONTIMEZONE, null, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("SESSIONTIMEZONE"));
    }

    @CsvSource({ "SYSTIMESTAMP", "CURRENT_TIMESTAMP" })
    @ParameterizedTest
    void visitSqlFunctionScalarTimestamp(final ScalarFunction scalarFunction) throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(CURRENT TIMESTAMP)"));
    }

    @Test
    void visitSqlFunctionScalarBitAnd() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.BIT_AND, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("BITAND('test')"));
    }

    @Test
    void visitSqlFunctionScalarBitToNum() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.BIT_TO_NUM, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("BIN_TO_NUM('test')"));
    }

    @Test
    void visitSqlFunctionScalarNullIfZero() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.NULLIFZERO, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("NULLIF('test', 0)"));
    }

    @Test
    void visitSqlFunctionScalarZeroIfNull() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.ZEROIFNULL, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("IFNULL('test', 0)"));
    }

    @Test
    void visitSqlFunctionScalarDiv() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        arguments.add(new SqlLiteralString("test2"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.DIV, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CAST(FLOOR('test' / FLOOR('test2')) AS DECIMAL(36, 0))"));
    }

    @Test
    void visitSqlFunctionScalarWithParent() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        arguments.add(new SqlLiteralString("test2"));
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"XML\"}").build();
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.DIV, arguments, true, false);
        sqlFunctionScalar.setParent(node);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CAST(FLOOR('test' / FLOOR('test2')) AS DECIMAL(36, 0))"));
    }

    @Test
    void visitSqlFunctionAggregate() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionAggregate sqlFunctionAggregate = new SqlFunctionAggregate(AggregateFunction.VAR_SAMP,
                arguments, false);
        assertThat(visitor.visit(sqlFunctionAggregate), equalTo("VARIANCE_SAMP('test')"));
    }

    @Test
    void visitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final List<Boolean> nulls = new ArrayList<>();
        nulls.add(false);
        nulls.add(true);
        final List<SqlNode> orderByArguments = new ArrayList<>();
        orderByArguments.add(new SqlLiteralString("orderBy1"));
        orderByArguments.add(new SqlLiteralString("orderBy2"));
        final SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat = new SqlFunctionAggregateGroupConcat(
                AggregateFunction.AVG, arguments, new SqlOrderBy(orderByArguments, nulls, null), false, "'");
        assertThat(visitor.visit(sqlFunctionAggregateGroupConcat),
                equalTo("LISTAGG('test', ''') WITHIN GROUP(ORDER BY 'orderBy1' DESC, 'orderBy2')"));
    }
}