package com.exasol.adapter.dialects.db2;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static utils.SqlNodesCreator.*;

import java.util.ArrayList;
import java.util.List;

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
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

@ExtendWith(MockitoExtension.class)
class DB2SqlGenerationVisitorTest {
    private SqlNodeVisitor<String> visitor;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        final SqlDialectFactory dialectFactory = new DB2SqlDialectFactory();
        final SqlDialect dialect = dialectFactory.createSqlDialect(connectionFactoryMock,
                AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new DB2SqlGenerationVisitor(dialect, context);
    }

    @Test
    void testVisitSqlColumnWithoutParent() throws AdapterException {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .build();
        final SqlColumn column = new SqlColumn(1, columnMetadata);
        assertThat(this.visitor.visit(column), equalTo("\"test_column\""));
    }

    @CsvSource(value = { "XML : XMLSERIALIZE(\"test_column\" as VARCHAR(32000) INCLUDING XMLDECLARATION)", //
            "CLOB : CAST(SUBSTRING(\"test_column\",32672) AS VARCHAR(32672))", //
            "CHAR () FOR BIT DATA : HEX(\"test_column\")", //
            "VARCHAR () FOR BIT DATA : HEX(\"test_column\")", //
            "TIME : VARCHAR(\"test_column\")", //
            "TIMESTAMP : VARCHAR(\"test_column\")" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlColumnWithParent(final String typeName, final String expected) throws AdapterException {
        final SqlColumn column = getSqlColumn(typeName);
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(this.visitor.visit(column), equalTo(expected));
    }

    private SqlColumn getSqlColumn(final String typeName) {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("test_column").type(DataType.createBool())
                .adapterNotes("{\"jdbcDataType\":2009, \"typeName\":\"" + typeName + "\"}").build();
        return new SqlColumn(1, columnMetadata);
    }

    @Test
    void testVisitSqlColumnWithParentTypeNameIsNotSupported() throws AdapterException {
        final SqlColumn column = getSqlColumn("BLOB");
        final SqlNode node = SqlSelectList.createSelectStarSelectList();
        column.setParent(node);
        assertThat(this.visitor.visit(column), equalTo("'BLOB NOT SUPPORTED'"));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlStatementSelect select = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        assertThat(this.visitor.visit(select), //
                equalTo("SELECT \"USER_ID\", " //
                        + "COUNT(\"URL\") FROM \"test_schema\".\"CLICKS\" " //
                        + "WHERE 1 < \"USER_ID\" " //
                        + "GROUP BY \"USER_ID\" " //
                        + "HAVING 1 < COUNT(\"URL\") " //
                        + "ORDER BY \"USER_ID\" FETCH FIRST 10 ROWS ONLY"));
    }

    @Test
    void testVisitSqlSelectListAnyValue() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertSqlNodeConvertedToOne(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithoutColumns();
        assertSqlNodeConvertedToAsterisk(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlSelectListSelectStarRequiresCast() throws AdapterException {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2009, \"typeName\":\"XML\"}", DataType.createVarChar(10, DataType.ExaCharset.UTF8),
                "test_column");
        assertThat(this.visitor.visit(sqlSelectList),
                equalTo("XMLSERIALIZE(\"test_column\" as VARCHAR(32000) INCLUDING XMLDECLARATION)"));
    }

    @Test
    void testVisitSqlSelectListSelectStarThrowsException() {
        final SqlSelectList sqlSelectList = createSqlSelectStarListWithOneColumn("",
                DataType.createVarChar(10, DataType.ExaCharset.UTF8), "test_column");
        assertThrows(SqlGenerationVisitorException.class, () -> this.visitor.visit(sqlSelectList));
    }

    @Test
    void testVisitSqlFunctionScalarTrimOneArgument() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ScalarFunction.TRIM, arguments, true, false);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("TRIM('test')"));
    }

    @Test
    void testVisitSqlFunctionScalarTrimOTwoArguments() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(ScalarFunction.TRIM,
                "ab cdef", "ab");
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("TRIM('ab' FROM 'ab cdef')"));
    }

    @CsvSource({ "ADD_DAYS, 10, 10 DAYS", //
            "ADD_HOURS, 10, 10 HOURS", //
            "ADD_MINUTES, 10, 10 MINUTES", //
            "ADD_SECONDS, 10, 10 SECONDS", //
            "ADD_YEARS, 10, 10 YEARS", //
            "ADD_WEEKS, 10, 70 DAYS" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarAddDateValues(final ScalarFunction scalarFunction, final int value,
            final String expected) throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, value);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("VARCHAR(\"test_column\" + " + expected + ")"));
    }

    @CsvSource({ "SYSDATE, CURRENT DATE", //
            "CURRENT_DATE, CURRENT DATE", //
            "DBTIMEZONE, DBTIMEZONE", //
            "LOCALTIMESTAMP, LOCALTIMESTAMP", //
            "SESSIONTIMEZONE, SESSIONTIMEZONE", //
            "SYSTIMESTAMP, VARCHAR(CURRENT TIMESTAMP)", //
            "CURRENT_TIMESTAMP, VARCHAR(CURRENT TIMESTAMP)" //
    })
    @ParameterizedTest
    void testVisitSqlFunctionScalar1(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, null, true, false);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @CsvSource(value = { "BIT_AND : BITAND('left', 'right')", //
            "BIT_TO_NUM : BIN_TO_NUM('left', 'right')", //
            "NULLIFZERO : NULLIF('left', 0)", //
            "ZEROIFNULL : IFNULL('left', 0)", //
            "DIV : CAST(FLOOR('left' / FLOOR('right')) AS DECIMAL(36, 0))"//
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlFunctionScalar2(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(scalarFunction,
                "left", "right");
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @Test
    void testVisitSqlFunctionScalarDiv() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(ScalarFunction.DIV,
                "left", "right");
        assertThat(this.visitor.visit(sqlFunctionScalar),
                equalTo("CAST(FLOOR('left' / FLOOR('right')) AS DECIMAL(36, 0))"));
    }

    @Test
    void testVisitSqlFunctionAggregate() throws AdapterException {
        final SqlFunctionAggregate sqlFunctionAggregate = createSqlFunctionAggregate();
        assertThat(this.visitor.visit(sqlFunctionAggregate), equalTo("AVG(DISTINCT \"test_column\")"));
    }

    @Test
    void testVisitSqlFunctionAggregateVarSamp() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionAggregate sqlFunctionAggregate = new SqlFunctionAggregate(AggregateFunction.VAR_SAMP,
                arguments, false);
        assertThat(this.visitor.visit(sqlFunctionAggregate), equalTo("VARIANCE_SAMP('test')"));
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlOrderBy orderBy = createSqlOrderByDescNullsFirst("test_column", "test_column2");
        final SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat = new SqlFunctionAggregateGroupConcat(
                AggregateFunction.AVG, arguments, orderBy, false, "'");
        assertThat(this.visitor.visit(sqlFunctionAggregateGroupConcat),
                equalTo("LISTAGG('test', ''') WITHIN GROUP(ORDER BY \"test_column\" DESC, \"test_column2\")"));
    }
}