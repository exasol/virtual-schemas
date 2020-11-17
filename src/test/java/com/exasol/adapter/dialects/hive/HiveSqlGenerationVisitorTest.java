package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import com.exasol.adapter.metadata.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.*;

@ExtendWith(MockitoExtension.class)
class HiveSqlGenerationVisitorTest {
    private SqlNodeVisitor<String> visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialectFactory dialectFactory = new HiveSqlDialectFactory();
        final SqlDialect dialect = dialectFactory.createSqlDialect(connectionFactoryMock,
                AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new HiveSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final TableMetadata tableMetadata = new TableMetadata("", "", Collections.emptyList(), "");
        final SqlTable fromClause = new SqlTable("test_table", tableMetadata);
        final SqlNode sqlStatementSelect = SqlStatementSelect.builder().selectList(sqlSelectList).fromClause(fromClause)
                .build();
        sqlSelectList.setParent(sqlStatementSelect);
        assertSqlNodeConvertedToAsterisk(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlSelectListSelectStarRequiresCastBinary() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":-2, \"typeName\":\"BINARY\"}")
                .type(DataType.createVarChar(10, DataType.ExaCharset.UTF8)).build());
        final TableMetadata tableMetadata = new TableMetadata("", "", columns, "");
        final SqlTable fromClause = new SqlTable("", tableMetadata);
        final SqlNode select = SqlStatementSelect.builder().selectList(sqlSelectList).fromClause(fromClause).build();
        sqlSelectList.setParent(select);
        assertThat(this.visitor.visit(sqlSelectList), equalTo("base64(`test_column`)"));
    }

    @Test
    void testVisitSqlSelectListRequiresAnyColumn() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertSqlNodeConvertedToOne(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlSelectListSelectRegularList() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList
                .createRegularSelectList(Arrays.asList(new SqlLiteralBool(true), new SqlLiteralString("string")));
        assertThat(this.visitor.visit(sqlSelectList), equalTo("true, 'string'"));
    }

    @Test
    void testVisitSqlSelectListSelectRegularListWithColumns() throws AdapterException {
        final ColumnMetadata columnMetadata1 = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":16, \"typeName\":\"BOOLEAN\"}").build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2")
                .type(DataType.createDouble()).adapterNotes("{\"jdbcDataType\":-2, \"typeName\":\"BINARY\"}").build();
        final SqlSelectList sqlSelectList = SqlSelectList.createRegularSelectList(Arrays.asList(
                new SqlColumn(1, columnMetadata1, "test_table"), new SqlColumn(2, columnMetadata2, "test_table")));
        assertThat(this.visitor.visit(sqlSelectList),
                equalTo("`test_table`.`test_column`, base64(`test_table`.`test_column2`)"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn("",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        SqlSelectList.createSelectStarSelectList();
        assertThrows(SqlGenerationVisitorException.class, () -> this.visitor.visit(sqlSelectList));
    }

    private SqlSelectList createSqlSelectStarListWithOneColumn(final String adapterNotes, final DataType dataType,
            final String columnName) {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name(columnName).adapterNotes(adapterNotes).type(dataType).build());
        final TableMetadata tableMetadata = new TableMetadata("", "", columns, "");
        final SqlTable fromClause = new SqlTable("", tableMetadata);
        final SqlNode sqlStatementSelect = SqlStatementSelect.builder().selectList(selectList).fromClause(fromClause)
                .build();
        selectList.setParent(sqlStatementSelect);
        return selectList;
    }

    @Test
    void testVisitSqlPredicateEqual() throws AdapterException {
        final SqlPredicateEqual sqlPredicateEqual = new SqlPredicateEqual(new SqlLiteralBool(true),
                new SqlLiteralBool(true));
        assertThat(this.visitor.visit(sqlPredicateEqual), equalTo("true = true"));
    }

    @Test
    void testVisitSqlPredicateEqualLeftNull() throws AdapterException {
        final SqlPredicateEqual sqlPredicateEqual = new SqlPredicateEqual(new SqlLiteralNull(),
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()));
        assertThat(this.visitor.visit(sqlPredicateEqual), equalTo("`test_column` IS NULL"));
    }

    @Test
    void testVisitSqlPredicateEqualRightNull() throws AdapterException {
        final SqlPredicateEqual sqlPredicateEqual = new SqlPredicateEqual(
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()),
                new SqlLiteralNull());
        assertThat(this.visitor.visit(sqlPredicateEqual), equalTo("`test_column` IS NULL"));
    }

    @Test
    void testVisitSqlPredicateNotEqual() throws AdapterException {
        final SqlPredicateNotEqual sqlPredicateNotEqual = new SqlPredicateNotEqual(new SqlLiteralBool(true),
                new SqlLiteralBool(false));
        assertThat(this.visitor.visit(sqlPredicateNotEqual), equalTo("true <> false"));
    }

    @Test
    void testVisitSqlPredicateEqualLeftNotNull() throws AdapterException {
        final SqlPredicateNotEqual sqlPredicateNotEqual = new SqlPredicateNotEqual(new SqlLiteralNull(),
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()));
        assertThat(this.visitor.visit(sqlPredicateNotEqual), equalTo("`test_column` IS NOT NULL"));
    }

    @Test
    void testVisitSqlPredicateEqualRightNotNull() throws AdapterException {
        final SqlPredicateNotEqual sqlPredicateNotEqual = new SqlPredicateNotEqual(
                new SqlColumn(0, ColumnMetadata.builder().name("test_column").type(DataType.createBool()).build()),
                new SqlLiteralNull());
        assertThat(this.visitor.visit(sqlPredicateNotEqual), equalTo("`test_column` IS NOT NULL"));
    }

    @Test
    void testVisitSqlPredicateLikeRegexp() throws AdapterException {
        final SqlPredicateLikeRegexp sqlSelectList = new SqlPredicateLikeRegexp(new SqlLiteralString("abcd"),
                new SqlLiteralString("a_d"));
        assertThat(this.visitor.visit(sqlSelectList), equalTo("'abcd'REGEXP'a_d'"));
    }

    @CsvSource({ "CONCAT", "REPEAT", "UPPER", "LOWER" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithCastedFunctions(final ScalarFunction scalarFunction) throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        arguments.add(new SqlLiteralDouble(10.10));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("CAST(" + scalarFunction.name() + "(10.5,10.1) as string)"));
    }

    @CsvSource({ "DIV, DIV", //
            "MOD, %", //
            "BIT_AND, &", //
            "BIT_OR, |", //
            "BIT_XOR, ^" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarWithChangedFunctions(final ScalarFunction scalarFunction,
            final String expectedString) throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        arguments.add(new SqlLiteralDouble(10.10));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("10.5 " + expectedString + " 10.1"));
    }

    @Test
    void testVisitSqlFunctionScalarSubstring() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("string"));
        arguments.add(new SqlLiteralDouble(1));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.SUBSTR, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("SUBSTR('string', 1.0)"));
    }

    @Test
    void testVisitSqlFunctionScalarSubstringWithFrom() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("string"));
        arguments.add(new SqlLiteralString("FROM 4 FOR 2"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.SUBSTR, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("SUBSTRING('string','FROM 4 FOR 2')"));
    }

    @Test
    void testVisitSqlFunctionScalarCurrentDate() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.CURRENT_DATE, null);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("CURRENT_DATE"));
    }

    @Test
    void testVisitSqlFunctionScalarDataTrunc() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDate("2019-07-04"));
        arguments.add(new SqlLiteralString("MM"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.DATE_TRUNC, arguments);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("TRUNC('MM',DATE '2019-07-04')"));
    }
}