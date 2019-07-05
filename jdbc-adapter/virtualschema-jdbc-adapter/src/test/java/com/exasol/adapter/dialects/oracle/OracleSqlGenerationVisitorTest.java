package com.exasol.adapter.dialects.oracle;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.mockito.*;

import java.math.*;
import java.sql.*;
import java.util.*;

import static com.exasol.adapter.sql.AggregateFunction.*;
import static com.exasol.adapter.sql.ScalarFunction.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static utils.SqlNodesCreator.*;

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
        assertThat(visitor.getAggregateFunctionsCast(), containsInAnyOrder(SUM, MIN, MAX, AVG, MEDIAN, FIRST_VALUE,
                LAST_VALUE, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP));
    }

    @Test
    void testGetScalarFunctionsCast() {
        assertThat(visitor.getScalarFunctionsCast(),
                containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, COS, COSH, COT,
                        DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH));
    }

    @Test
    void testVisitSqlStatementSelect() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createAnyValueSelectList();
        final SqlStatementSelect sqlStatementSelect = createSqlStatementSelect(selectList, Collections.EMPTY_LIST,
                "test_table_name");
        assertThat(visitor.visit(sqlStatementSelect), equalTo("SELECT 1 FROM \"test_schema\".\"test_table_name\""));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitAnyValue() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createAnyValueSelectList();
        final SqlTable fromClause = createFromClause(Collections.EMPTY_LIST, "");
        final SqlLimit limit = new SqlLimit(10, 3);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(visitor.visit(sqlStatementSelect), equalTo("1"));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitSelectStar() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlTable fromClause = createFromClause(Collections.EMPTY_LIST, "test_table_name");
        final SqlLimit limit = new SqlLimit(10, 3);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(visitor.visit(sqlStatementSelect),
                equalTo("SELECT  FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM "
                        + "ROWNUM_SUB FROM ( SELECT  FROM \"test_schema\".\"test_table_name\"  ) LIMIT_SUBSELECT WHERE "
                        + "ROWNUM <= 13 ) WHERE ROWNUM_SUB > 3"));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitRegularSelectList() throws AdapterException {
        final SqlSelectList selectList = createRegularSqlSelectListWithTwoColumns();
        final SqlTable fromClause = createFromClause(Collections.EMPTY_LIST, "test_table_name");
        final SqlLimit limit = new SqlLimit(10, 3);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(visitor.visit(sqlStatementSelect),
                equalTo("SELECT c0, c1 FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM "
                        + "ROWNUM_SUB FROM ( SELECT true AS c0, 'string' AS c1 FROM \"test_schema\".\"test_table_name\""
                        + "  ) LIMIT_SUBSELECT WHERE ROWNUM <= 13 ) WHERE ROWNUM_SUB > 3"));
    }

    @Test
    void testVisitSqlStatementSelectWithLimitRegularSelectListWithoutOffset() throws AdapterException {
        final SqlSelectList selectList = createRegularSqlSelectListWithTwoColumns();
        final SqlTable fromClause = createFromClause(Collections.EMPTY_LIST, "test_table_name");
        final SqlLimit limit = new SqlLimit(10);
        final SqlStatementSelect sqlStatementSelect = new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, limit);
        assertThat(visitor.visit(sqlStatementSelect),
                equalTo("SELECT LIMIT_SUBSELECT.* FROM ( SELECT true, 'string' FROM \"test_schema\""
                        + ".\"test_table_name\"  ) LIMIT_SUBSELECT WHERE ROWNUM <= 10"));
    }

    @Test
    void testVisitSqlSelectListRequiresAnyColumn() throws AdapterException {
        final SqlSelectList sqlSelectList = SqlSelectList.createAnyValueSelectList();
        assertThat(visitor.visit(sqlSelectList), equalTo("1"));
    }

    @Test
    void testVisitSqlSelectListSelectStar() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":16, \"typeName\":\"BOOLEAN\"}").type(DataType.createBool()).build());
        final SqlNode sqlStatementSelect = createSqlStatementSelect(selectList, columns, "");
        selectList.setParent(sqlStatementSelect);
        assertThat(visitor.visit(selectList), equalTo("*"));
    }

    @CsvSource({ "NUMBER", "TIMESTAMP", "INTERVAL", "BINARY_FLOAT", "BINARY_DOUBLE", "CLOB", "NCLOB" })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarCastToChar(final String dataType) throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2, \"typeName\":\"" + dataType + "\"}")
                .type(DataType.createVarChar(50, DataType.ExaCharset.UTF8)).build());
        final SqlNode select = createSqlStatementSelect(selectList, columns, "");
        selectList.setParent(select);
        assertThat(visitor.visit(selectList), equalTo("TO_CHAR(\"test_column\")"));
    }

    @CsvSource({ "ROWID", "UROWID" })
    @ParameterizedTest
    void testVisitSqlSelectListSelectStarNumberCastRowidToChar(final String dataType) throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("test_column")
                .adapterNotes("{\"jdbcDataType\":2, \"typeName\":\"" + dataType + "\"}")
                .type(DataType.createVarChar(50, DataType.ExaCharset.UTF8)).build());
        final SqlNode select = createSqlStatementSelect(selectList, columns, "");
        selectList.setParent(select);
        assertThat(visitor.visit(selectList), equalTo("ROWIDTOCHAR(\"test_column\")"));
    }

    @Test
    void testVisitSqlSelectListSelectStarNumberCastBlobToChar() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(
                ColumnMetadata.builder().name("test_column").adapterNotes("{\"jdbcDataType\":2, \"typeName\":\"BLOB\"}")
                        .type(DataType.createVarChar(50, DataType.ExaCharset.UTF8)).build());
        final SqlNode select = createSqlStatementSelect(selectList, columns, "");
        selectList.setParent(select);
        assertThat(visitor.visit(selectList), equalTo("UTL_RAW.CAST_TO_VARCHAR2(\"test_column\")"));
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
        assertThat(visitor.visit(selectList), equalTo("CAST(\"test_column\" AS DECIMAL(0,0))"));
    }

    @Test
    void testVisitSqlSelectListRegularSelectList() throws AdapterException {
        final SqlSelectList selectList = createRegularSqlSelectListWithTwoColumns();
        final SqlTable fromClause = createFromClause(Collections.EMPTY_LIST, "");
        final SqlNode select = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);
        selectList.setParent(select);
        assertThat(visitor.visit(selectList), equalTo("true, 'string'"));
    }

    @Test
    void testVisitSqlPredicateLikeRegexp() throws AdapterException {
        final SqlPredicateLikeRegexp sqlSelectList = new SqlPredicateLikeRegexp(new SqlLiteralString("abcd"),
                new SqlLiteralString("a_d"));
        assertThat(visitor.visit(sqlSelectList), equalTo("REGEXP_LIKE('abcd', 'a_d')"));
    }

    @Test
    void testVisitSqlLiteralExactnumeric() {
        final SqlLiteralExactnumeric literalExactnumeric = new SqlLiteralExactnumeric(new BigDecimal(5.9));
        assertThat(visitor.visit(literalExactnumeric), equalTo("5.9000000000000003552713678800500929355621337890625"));
    }

    @Test
    void testVisitSqlLiteralExactnumericInSelectList() {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlLiteralExactnumeric literalExactnumeric = new SqlLiteralExactnumeric(new BigDecimal(5.9));
        literalExactnumeric.setParent(selectList);
        assertThat(visitor.visit(literalExactnumeric),
                equalTo("TO_CHAR(5.9000000000000003552713678800500929355621337890625)"));
    }

    @Test
    void testVisitSqlLiteralDouble() {
        final SqlLiteralDouble literalDouble = new SqlLiteralDouble(10.6);
        assertThat(visitor.visit(literalDouble), equalTo("10.6"));
    }

    @Test
    void testVisitSqlLiteralDoubleInSelectList() {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlLiteralDouble literalDouble = new SqlLiteralDouble(10.6);
        literalDouble.setParent(selectList);
        assertThat(visitor.visit(literalDouble), equalTo("TO_CHAR(10.6)"));
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        final SqlFunctionAggregateGroupConcat aggregateGroupConcat = new SqlFunctionAggregateGroupConcat(AVG, arguments,
                null, true, "'");
        assertThat(visitor.visit(aggregateGroupConcat), equalTo("LISTAGG(10.5, ''') WITHIN GROUP(ORDER BY 10.5)"));
    }

    @Test
    void testVisitSqlFunctionAggregateGroupConcatWithOrderBy() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralDouble(10.5));
        final SqlOrderBy orderBy = createSqlOrderByDescNullsFirst();
        final SqlFunctionAggregateGroupConcat aggregateGroupConcat = new SqlFunctionAggregateGroupConcat(AVG, arguments,
                orderBy, true, "'");
        assertThat(visitor.visit(aggregateGroupConcat), equalTo(
                "LISTAGG(10.5, ''') WITHIN GROUP(ORDER BY \"test_column\" DESC NULLS FIRST, \"test_column2\")"));
    }

    @Test
    void testVisitSqlFunctionAggregate() throws AdapterException {
        final SqlFunctionAggregate sqlFunctionAggregate = createSqlFunctionAggregate();
        assertThat(visitor.visit(sqlFunctionAggregate), equalTo("AVG(DISTINCT \"test_column\")"));
    }

    @Test
    void testVisitSqlFunctionAggregateInSelectList() throws AdapterException {
        final SqlFunctionAggregate sqlFunctionAggregate = createSqlFunctionAggregate();
        final SqlNode selectList = SqlSelectList.createSelectStarSelectList();
        sqlFunctionAggregate.setParent(selectList);
        assertThat(visitor.visit(sqlFunctionAggregate), equalTo("CAST(AVG(DISTINCT \"test_column\") AS FLOAT)"));
    }

    @Test
    void testVisitSqlFunctionScalarLocateThreeArguments() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("ab "));
        arguments.add(new SqlLiteralString("ab cdef"));
        arguments.add(new SqlLiteralString("ab cdef rty"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(LOCATE, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("INSTR('ab cdef', 'ab ', 'ab cdef rty')"));
    }

    @Test
    void testVisitSqlFunctionScalarTrimOneArgument() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(TRIM, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("TRIM('test')"));
    }

    @Test
    void testVisitSqlFunctionScalarTrimOTwoArguments() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(TRIM, "ab cdef",
                "ab");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("TRIM('ab' FROM 'ab cdef')"));
    }

    @Test
    void testVisitSqlFunctionScalarAddDays() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ADD_DAYS, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL '10' DAY)"));
    }

    @Test
    void testVisitSqlFunctionScalarAddHours() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ADD_HOURS, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL '10' HOUR)"));
    }

    @Test
    void testVisitSqlFunctionScalarAddMinutes() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ADD_MINUTES, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL '10' MINUTE)"));
    }

    @Test
    void testVisitSqlFunctionScalarAddSeconds() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ADD_SECONDS, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL '10' SECOND)"));
    }

    @Test
    void testVisitSqlFunctionScalarAddYears() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ADD_YEARS, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL '10' YEAR)"));
    }

    @Test
    void testVisitSqlFunctionScalarAddWeeks() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlColumn(1,
                ColumnMetadata.builder().name("test_column").type(DataType.createChar(20, DataType.ExaCharset.UTF8))
                        .adapterNotes("{\"jdbcDataType\":93, " + "\"typeName\":\"TIMESTAMP\"}").build()));
        arguments.add(new SqlLiteralExactnumeric(new BigDecimal(10)));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ADD_WEEKS, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(\"test_column\" + INTERVAL '70' DAY)"));
    }

    @Test
    void testVisitSqlFunctionScalarCurrentDate() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(CURRENT_DATE, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CURRENT_DATE"));
    }

    @Test
    void testVisitSqlFunctionScalarCurrentTimestamp() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(CURRENT_TIMESTAMP, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CURRENT_TIMESTAMP"));
    }

    @Test
    void testVisitSqlFunctionScalarDbTimezone() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(DBTIMEZONE, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("DBTIMEZONE"));
    }

    @Test
    void testVisitSqlFunctionScalarLocalTimestamp() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(LOCALTIMESTAMP, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("LOCALTIMESTAMP"));
    }

    @Test
    void testVisitSqlFunctionScalarSessionTimezone() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(SESSIONTIMEZONE, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("SESSIONTIMEZONE"));
    }

    @Test
    void testVisitSqlFunctionScalarSysdate() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(SYSDATE, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("TO_DATE(SYSDATE)"));
    }

    @Test
    void testVisitSqlFunctionScalarSysdtemstamp() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(SYSTIMESTAMP, null, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("SYSTIMESTAMP"));
    }

    @Test
    void testVisitSqlFunctionScalarBitAnd() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(BIT_AND, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("BITAND('test')"));
    }

    @Test
    void testVisitSqlFunctionScalarBitToNum() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(BIT_TO_NUM, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("BIN_TO_NUM('test')"));
    }

    @Test
    void testVisitSqlFunctionScalarNullIfZero() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(NULLIFZERO, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("NULLIF('test', 0)"));
    }

    @Test
    void testVisitSqlFunctionScalarZeroIfNull() throws AdapterException {
        final List<SqlNode> arguments = new ArrayList<>();
        arguments.add(new SqlLiteralString("test"));
        final SqlFunctionScalar sqlFunctionScalar = new SqlFunctionScalar(ZEROIFNULL, arguments, true, false);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("NVL('test', 0)"));
    }

    @Test
    void testVisitSqlFunctionScalarDiv() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(DIV, "test", "test2");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CAST(FLOOR('test' / 'test2') AS NUMBER(36, 0))"));
    }

    @Test
    void testVisitSqlFunctionScalarCot() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(COT, "test", "");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(1 / TAN('test'))"));
    }

    @Test
    void testVisitSqlFunctionScalarDegrees() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(DEGREES, "test", "");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(('test') * 180 / ACOS(-1))"));
    }

    @Test
    void testVisitSqlFunctionScalarRadians() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(RADIANS, "test", "");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("(('test') * ACOS(-1) / 180)"));
    }

    @Test
    void testVisitSqlFunctionScalarRepeat() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(REPEAT, "test",
                "test2");
        assertThat(visitor.visit(sqlFunctionScalar),
                equalTo("RPAD(TO_CHAR('test'), LENGTH('test') * ROUND('test2'), 'test')"));
    }

    @Test
    void testVisitSqlFunctionScalarReverse() throws AdapterException {
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(REVERSE, "test", "");
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("REVERSE(TO_CHAR('test'))"));
    }

    @Test
    void testVisitSqlFunctionScalarInSelectList() throws AdapterException {
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlFunctionScalar sqlFunctionScalar = createSqlFunctionScalarWithTwoStringArguments(TANH, "test", "");
        sqlFunctionScalar.setParent(selectList);
        assertThat(visitor.visit(sqlFunctionScalar), equalTo("CAST(TANH('test', '') AS FLOAT)"));
    }
}