package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToAsterisk;
import static com.exasol.adapter.dialects.VisitorAssertions.assertSqlNodeConvertedToOne;
import static com.exasol.adapter.sql.AggregateFunction.*;
import static com.exasol.adapter.sql.ScalarFunction.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static utils.SqlNodesCreator.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

class OracleSqlGenerationVisitorTest {
    private OracleSqlGenerationVisitor visitor;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new OracleSqlDialect(this.connectionMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new OracleSqlGenerationVisitor(dialect, context);
    }

    @Test
    void testGetAggregateFunctionsCast() {
        assertThat(this.visitor.getAggregateFunctionsCast(), containsInAnyOrder(SUM, MIN, MAX, AVG, MEDIAN, FIRST_VALUE,
                LAST_VALUE, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP));
    }

    @Test
    void testGetScalarFunctionsCast() {
        assertThat(this.visitor.getScalarFunctionsCast(),
                containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, COS, COSH, COT,
                        DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createAnyValueSelectList();
        final SqlStatementSelect sqlStatementSelect = createSqlStatementSelect(selectList, Collections.emptyList(),
                "test_table_name");
        assertThat(this.visitor.visit(sqlStatementSelect),
                equalTo("SELECT 1 FROM \"test_schema\".\"test_table_name\""));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitAnyValue() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createAnyValueSelectList();
        final SqlTable fromClause = createFromClause(Collections.emptyList(), "");
        final SqlLimit limit = new SqlLimit(10, 3);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(this.visitor.visit(sqlStatementSelect), equalTo("1"));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitSelectStar() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlTable fromClause = createFromClause(Collections.emptyList(), "test_table_name");
        final SqlLimit limit = new SqlLimit(10, 3);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(this.visitor.visit(sqlStatementSelect),
                equalTo("SELECT  FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM "
                        + "ROWNUM_SUB FROM ( SELECT  FROM \"test_schema\".\"test_table_name\"  ) LIMIT_SUBSELECT WHERE "
                        + "ROWNUM <= 13 ) WHERE ROWNUM_SUB > 3"));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitRegularSelectList() throws AdapterException {
        final SqlSelectList selectList = createRegularSqlSelectListWithTwoColumns();
        final SqlTable fromClause = createFromClause(Collections.emptyList(), "test_table_name");
        final SqlLimit limit = new SqlLimit(10, 3);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(this.visitor.visit(sqlStatementSelect),
                equalTo("SELECT c0, c1 FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM "
                        + "ROWNUM_SUB FROM ( SELECT true AS c0, 'string' AS c1 FROM \"test_schema\".\"test_table_name\""
                        + "  ) LIMIT_SUBSELECT WHERE ROWNUM <= 13 ) WHERE ROWNUM_SUB > 3"));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitRegularSelectListWithoutOffset() throws AdapterException {
        final SqlSelectList selectList = createRegularSqlSelectListWithTwoColumns();
        final SqlTable fromClause = createFromClause(Collections.emptyList(), "test_table_name");
        final SqlLimit limit = new SqlLimit(10);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(this.visitor.visit(sqlStatementSelect),
                equalTo("SELECT LIMIT_SUBSELECT.* FROM ( SELECT true, 'string' FROM \"test_schema\""
                        + ".\"test_table_name\"  ) LIMIT_SUBSELECT WHERE ROWNUM <= 10"));
    }

    @Test
    void testVisitSqlSelectListRequiresAnyColumn() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertSqlNodeConvertedToOne(sqlSelectList, this.visitor);
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList selectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":16, \"typeName\":\"BOOLEAN\"}", DataType.createBool(), "test_column");
        assertSqlNodeConvertedToAsterisk(selectList, this.visitor);
    }

    @CsvSource({ "NUMBER", "INTERVAL", "BINARY_FLOAT", "BINARY_DOUBLE", "CLOB", "NCLOB" })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarCastToChar(final String dataType) throws AdapterException {
        final SqlSelectList selectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2, \"typeName\":\"" + dataType + "\"}",
                DataType.createVarChar(50, DataType.ExaCharset.UTF8), "test_column");
        assertThat(this.visitor.visit(selectList), equalTo("TO_CHAR(\"test_column\")"));
    }

    @Test
    void testVisitSqlSelectListSelectStarWithTimestamp() throws AdapterException {
        final SqlSelectList selectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2, \"typeName\":\"TIMESTAMP\"}",
                DataType.createVarChar(50, DataType.ExaCharset.UTF8), "test_column");
        assertThat(this.visitor.visit(selectList), equalTo(
                "TO_TIMESTAMP(TO_CHAR(\"test_column\", 'YYYY-MM-DD HH24:MI:SS.FF3'), 'YYYY-MM-DD HH24:MI:SS.FF3')"));
    }

    @CsvSource({ "ROWID", "UROWID" })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarNumberCastRowidToChar(final String dataType) throws AdapterException {
        final SqlSelectList selectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2, \"typeName\":\"" + dataType + "\"}",
                DataType.createVarChar(50, DataType.ExaCharset.UTF8), "test_column");
        assertThat(this.visitor.visit(selectList), equalTo("ROWIDTOCHAR(\"test_column\")"));
    }

    @Test
    void testVisitSqlSelectListSelectStarNumberCastBlobToChar() throws AdapterException {
        final SqlSelectList selectList = createSqlSelectStarListWithOneColumn(
                "{\"jdbcDataType\":2, \"typeName\":\"BLOB\"}", DataType.createVarChar(50, DataType.ExaCharset.UTF8),
                "test_column");
        assertThat(this.visitor.visit(selectList),
                equalTo("UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_DECODE(\"test_column\"))"));
    }

    @Test
    void testVisitSqlSelectListSelectStarNumberCastToDecimal() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2, \"typeName\":\"NUMBER\"}").type(DataType.createDouble()).build());
        final SqlTable fromClause = createFromClause(columns, "");
        final SqlNode select = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);
        selectList.setParent(select);
        assertThat(this.visitor.visit(selectList), equalTo("CAST(\"test_column\" AS DECIMAL(0,0))"));
    }

    @Test
    void testVisitSqlSelectListRegularSelectList() throws AdapterException {
        final SqlSelectList selectList = createRegularSqlSelectListWithTwoColumns();
        final SqlTable fromClause = createFromClause(Collections.emptyList(), "");
        final SqlNode select = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);
        selectList.setParent(select);
        assertThat(this.visitor.visit(selectList), equalTo("true, 'string'"));
    }

    @Test
    void testVisitSqlPredicateLikeRegexp() throws AdapterException {
        final SqlPredicateLikeRegexp sqlSelectList = new SqlPredicateLikeRegexp(new SqlLiteralString("abcd"),
                new SqlLiteralString("a_d"));
        assertThat(this.visitor.visit(sqlSelectList), equalTo("REGEXP_LIKE('abcd', 'a_d')"));
    }

    @Test
    void testVisitSqlLiteralExactnumeric() {
        final SqlLiteralExactnumeric literalExactnumeric = new SqlLiteralExactnumeric(new BigDecimal(5.9));
        assertThat(this.visitor.visit(literalExactnumeric),
                equalTo("5.9000000000000003552713678800500929355621337890625"));
    }

    @Test
    void testVisitSqlLiteralExactnumericInSelectList() {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlLiteralExactnumeric literalExactnumeric = new SqlLiteralExactnumeric(new BigDecimal(5.9));
        literalExactnumeric.setParent(selectList);
        assertThat(this.visitor.visit(literalExactnumeric),
                equalTo("TO_CHAR(5.9000000000000003552713678800500929355621337890625)"));
    }

    @Test
    void testVisitSqlLiteralDouble() {
        final SqlLiteralDouble literalDouble = new SqlLiteralDouble(10.6);
        assertThat(this.visitor.visit(literalDouble), equalTo("10.6"));
    }

    @Test
    void testVisitSqlLiteralDoubleInSelectList() {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlLiteralDouble literalDouble = new SqlLiteralDouble(10.6);
        literalDouble.setParent(selectList);
        assertThat(this.visitor.visit(literalDouble), equalTo("TO_CHAR(10.6)"));
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        final SqlFunctionAggregateGroupConcat aggregateGroupConcat = new SqlFunctionAggregateGroupConcat(AVG, arguments,
                null, true, "'");
        assertThat(this.visitor.visit(aggregateGroupConcat), equalTo("LISTAGG(10.5, ''') WITHIN GROUP(ORDER BY 10.5)"));
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcatWithOrderBy() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        final SqlOrderBy orderBy = createSqlOrderByDescNullsFirst("test_column", "test_column2");
        final SqlFunctionAggregateGroupConcat aggregateGroupConcat = new SqlFunctionAggregateGroupConcat(AVG, arguments,
                orderBy, true, "'");
        assertThat(this.visitor.visit(aggregateGroupConcat), equalTo(
                "LISTAGG(10.5, ''') WITHIN GROUP(ORDER BY \"test_column\" DESC NULLS FIRST, \"test_column2\")"));
    }

    @Test
    void testVisitSqlFunctionAggregate() throws AdapterException {
        final SqlFunctionAggregate sqlFunctionAggregate = createSqlFunctionAggregate();
        assertThat(this.visitor.visit(sqlFunctionAggregate), equalTo("AVG(DISTINCT \"test_column\")"));
    }

    @Test
    void testVisitSqlFunctionAggregateInSelectList() throws AdapterException {
        final SqlFunctionAggregate sqlFunctionAggregate = createSqlFunctionAggregate();
        final SqlNode selectList = SqlSelectList.createSelectStarSelectList();
        sqlFunctionAggregate.setParent(selectList);
        assertThat(this.visitor.visit(sqlFunctionAggregate), equalTo("CAST(AVG(DISTINCT \"test_column\") AS FLOAT)"));
    }

    @Test
    void testVisitSqlFunctionScalarLocateThreeArguments() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("ab "));
        arguments.add(new SqlLiteralString("ab cdef"));
        arguments.add(new SqlLiteralString("ab cdef rty"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(LOCATE, arguments, true, false);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("INSTR('ab cdef', 'ab ', 'ab cdef rty')"));
    }

    @Test
    void testVisitSqlFunctionScalarTrimOneArgument() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(TRIM, arguments, true, false);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("TRIM('test')"));
    }

    @Test
    void testVisitSqlFunctionScalarTrimOTwoArguments() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(TRIM, "ab cdef",
                "ab");
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("TRIM('ab' FROM 'ab cdef')"));
    }

    @CsvSource({ "ADD_DAYS, '10' DAY", //
            "ADD_HOURS,  '10' HOUR", //
            "ADD_MINUTES,  '10' MINUTE", //
            "ADD_SECONDS,  '10' SECOND", //
            "ADD_YEARS,  '10' YEAR", //
            "ADD_WEEKS, '70' DAY" })
    @ParameterizedTest
    void testVisitSqlFunctionScalarAddDateValues(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarForDateTest(scalarFunction, 10);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL " + expected + ")"));
    }

    @CsvSource({ "CURRENT_DATE, CURRENT_DATE", //
            "CURRENT_TIMESTAMP,  CURRENT_TIMESTAMP", //
            "DBTIMEZONE,  DBTIMEZONE", //
            "LOCALTIMESTAMP,  LOCALTIMESTAMP", //
            "SESSIONTIMEZONE, SESSIONTIMEZONE", //
            "SYSDATE, TO_DATE(SYSDATE)", //
            "SYSTIMESTAMP, SYSTIMESTAMP" })
    @ParameterizedTest
    void testVisitSqlFunctionScalar1(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(scalarFunction, null, true, false);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @CsvSource(value = { "BIT_AND : BITAND('left', 'right')", //
            "BIT_TO_NUM : BIN_TO_NUM('left', 'right')", //
            "NULLIFZERO : NULLIF('left', 0)", //
            "ZEROIFNULL : NVL('left', 0)", //
            "DIV : CAST(FLOOR('left' / 'right') AS NUMBER(36, 0))", //
            "COT : (1 / TAN('left'))", //
            "DEGREES : (('left') * 180 / ACOS(-1))", //
            "RADIANS : (('left') * ACOS(-1) / 180)", //
            "REPEAT : RPAD(TO_CHAR('left'), LENGTH('left') * ROUND('right'), 'left')", //
            "REVERSE : REVERSE(TO_CHAR('left'))" //
    }, delimiter = ':')
    @ParameterizedTest
    void testVisitSqlFunctionScalar2(final ScalarFunction scalarFunction, final String expected)
            throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(scalarFunction,
                "left", "right");
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo(expected));
    }

    @Test
    void testVisitSqlFunctionScalarInSelectList() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(TANH, "test", "");
        sqlFunctionScalar.setParent(selectList);
        assertThat(this.visitor.visit(sqlFunctionScalar), equalTo("CAST(TANH('test', '') AS FLOAT)"));
    }
}