package com.exasol.adapter.dialects.hive;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.mockito.*;

import java.sql.*;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HiveSqlGenerationVisitorTest {
    private SqlGenerationVisitor visitor;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new HiveSqlDialect(this.connectionMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new HiveSqlGenerationVisitor(dialect, context);
    }

    @Test
    void visitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final TableMetadata tableMetadata = new TableMetadata("", "", Collections.EMPTY_LIST, "");
        final SqlTable fromClause = new SqlTable("", tableMetadata);
        final SqlNode select = new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
        sqlSelectList.setParent(select);
        assertThat(visitor.visit(sqlSelectList), equalTo("*"));
    }

    @Test
    void visitSqlSelectListSelectStarRequiresCastBinary() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":-2, \"typeName\":\"BINARY\"}")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final TableMetadata tableMetadata = new TableMetadata("", "", columns, "");
        final SqlTable fromClause = new SqlTable("", tableMetadata);
        final SqlNode select = new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
        sqlSelectList.setParent(select);
        assertThat(visitor.visit(sqlSelectList), equalTo("base64(`test_column`)"));
    }

    @Test
    void visitSqlSelectListRequiresAnyColumn() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(visitor.visit(sqlSelectList), equalTo("1"));
    }

    @Test
    void visitSqlSelectListSelectRegularList() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList
                .createRegularSelectList(Arrays.asList(new SqlLiteralBool(true), new SqlLiteralString("string")));
        assertThat(visitor.visit(sqlSelectList), equalTo("true, 'string'"));
    }

    @Test
    void visitSqlSelectListSelectRegularListWithColumns() throws AdapterException {
        final ColumnMetadata columnMetadata1 = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":16, \"typeName\":\"BOOLEAN\"}").build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2")
                .type(DataType.createDouble()).adapterNotes("{\"jdbcDataType\":-2, \"typeName\":\"BINARY\"}").build();
        final SqlSelectList sqlSelectList = SqlSelectList.createRegularSelectList(Arrays.asList(
                new SqlColumn(1, columnMetadata1, "test_table"), new SqlColumn(2, columnMetadata2, "test_table")));
        assertThat(visitor.visit(sqlSelectList),
                equalTo("`test_table`.`test_column`, base64(`test_table`.`test_column2`)"));
    }

    @Test
    void visitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final TableMetadata tableMetadata = new TableMetadata("", "", columns, "");
        final SqlTable fromClause = new SqlTable("", tableMetadata);
        final SqlNode select = new SqlStatementSelect(fromClause, sqlSelectList, null, null, null, null, null);
        sqlSelectList.setParent(select);
        assertThrows(SqlGenerationVisitorException.class, () -> visitor.visit(sqlSelectList));
    }

    @Test
    void visitSqlPredicateEqual() throws AdapterException {
        final SqlPredicateEqual sqlPredicateEqual = new SqlPredicateEqual(new SqlLiteralBool(true),
                new SqlLiteralBool(true));
        assertThat(visitor.visit(sqlPredicateEqual), equalTo("true = true"));
    }

    @Test
    void visitSqlPredicateEqualLeftNull() throws AdapterException {
        final SqlPredicateEqual sqlPredicateEqual = new SqlPredicateEqual(new SqlLiteralNull(),
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()));
        assertThat(visitor.visit(sqlPredicateEqual), equalTo("`test_column` IS NULL"));
    }

    @Test
    void visitSqlPredicateEqualRightNull() throws AdapterException {
        final SqlPredicateEqual sqlPredicateEqual = new SqlPredicateEqual(
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()),
                new SqlLiteralNull());
        assertThat(visitor.visit(sqlPredicateEqual), equalTo("`test_column` IS NULL"));
    }

    @Test
    void visitSqlPredicateNotEqual() throws AdapterException {
        final SqlPredicateNotEqual sqlPredicateNotEqual = new SqlPredicateNotEqual(new SqlLiteralBool(true),
                new SqlLiteralBool(false));
        assertThat(visitor.visit(sqlPredicateNotEqual), equalTo("true <> false"));
    }

    @Test
    void visitSqlPredicateEqualLeftNotNull() throws AdapterException {
        final SqlPredicateNotEqual sqlPredicateNotEqual = new SqlPredicateNotEqual(new SqlLiteralNull(),
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()));
        assertThat(visitor.visit(sqlPredicateNotEqual), equalTo("`test_column` IS NOT NULL"));
    }

    @Test
    void visitSqlPredicateEqualRightNotNull() throws AdapterException {
        final SqlPredicateNotEqual sqlPredicateNotEqual = new SqlPredicateNotEqual(
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()),
                new SqlLiteralNull());
        assertThat(visitor.visit(sqlPredicateNotEqual), equalTo("`test_column` IS NOT NULL"));
    }

    @Test
    void visitSqlPredicateLikeRegexp() throws AdapterException {
        final SqlPredicateLikeRegexp sqlSelectList = new SqlPredicateLikeRegexp(new SqlLiteralString("abcd"),
                new SqlLiteralString("a_d"));
        assertThat(visitor.visit(sqlSelectList), equalTo("'abcd'REGEXP'a_d'"));
    }

    @CsvSource({ "CONCAT", "REPEAT", "UPPER", "LOWER" })
    @ParameterizedTest
    void visitSqlFunctionScalarWithCastedFunctions(final ScalarFunction scalarFunction) throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        arguments.add(new SqlLiteralDouble(10.10));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar),
                equalTo("CAST(" + scalarFunction.name() + "(10.5,10.1) as string)"));
    }

    @CsvSource({ "DIV, DIV", //
            "MOD, %", //
            "BIT_AND, &", //
            "BIT_OR, |", //
            "BIT_XOR, ^" })
    @ParameterizedTest
    void visitSqlFunctionScalarWithChangedFunctions(final ScalarFunction scalarFunction, final String expectedString)
            throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        arguments.add(new SqlLiteralDouble(10.10));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("10.5 " + expectedString + " 10.1"));
    }

    @Test
    void visitSqlFunctionScalarSubstring() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("string"));
        arguments.add(new SqlLiteralDouble(1));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.SUBSTR, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("SUBSTR('string', 1.0)"));
    }

    @Test
    void visitSqlFunctionScalarSubstringWithFrom() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("string"));
        arguments.add(new SqlLiteralString("FROM 4 FOR 2"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.SUBSTR, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("SUBSTRING('string','FROM 4 FOR 2')"));
    }

    @Test
    void visitSqlFunctionScalarCurrentDate() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.CURRENT_DATE, null, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CURRENT_DATE"));
    }

    @Test
    void visitSqlFunctionScalarDataTrunc() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDate("2019-07-04"));
        arguments.add(new SqlLiteralString("MM"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.DATE_TRUNC, arguments, true,
                false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("TRUNC('MM',DATE '2019-07-04')"));
    }
}