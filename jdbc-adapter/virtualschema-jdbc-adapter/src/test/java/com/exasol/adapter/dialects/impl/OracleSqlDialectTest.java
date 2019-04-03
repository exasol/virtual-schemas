package com.exasol.adapter.dialects.impl;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

import utils.SqlTestUtil;

class OracleSqlDialectTest {
    private static final String SCHEMA_NAME = "SCHEMA";
    private SqlNode node;
    private SqlDialect dialect;
    private SqlGenerationVisitor generator;

    @BeforeEach
    void beforeEach() {
        this.node = DialectTestData.getTestSqlNode();
        this.dialect = new OracleSqlDialect(null, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("", SCHEMA_NAME, false, false);
        this.generator = this.dialect.getSqlGenerationVisitor(context);
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(
                () -> assertThat(capabilities.getMainCapabilities(),
                        containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                                AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                                AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                                LIMIT_WITH_OFFSET)), //
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING,
                                INTERVAL)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE,
                                REGEXP_LIKE, BETWEEN, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR,
                                GROUP_CONCAT_ORDER_BY, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT, MEDIAN,
                                FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_SAMP, VARIANCE,
                                VARIANCE_DISTINCT, VAR_POP, VAR_SAMP)), //
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(CEIL, DIV, FLOOR, SIGN, ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN,
                                ATAN, ATAN2, COS, COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER,
                                RADIANS, SIN, SINH, SQRT, TAN, TANH, ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD,
                                LTRIM, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RPAD,
                                RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES,
                                ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP,
                                DBTIMEZONE, LOCALTIMESTAMP, NUMTODSINTERVAL, NUMTOYMINTERVAL, SESSIONTIMEZONE, SYSDATE,
                                SYSTIMESTAMP, CAST, TO_CHAR, TO_DATE, TO_DSINTERVAL, TO_YMINTERVAL, TO_NUMBER,
                                TO_TIMESTAMP, BIT_AND, BIT_TO_NUM, CASE, NULLIFZERO, ZEROIFNULL)));
    }

    @Test
    void testSqlGeneratorWithLimit() throws AdapterException {
        final String expectedSql = "SELECT LIMIT_SUBSELECT.* FROM ( " + //
                "  SELECT \"USER_ID\", COUNT(\"URL\") " + //
                "    FROM \"SCHEMA\".\"CLICKS\"" + //
                "    WHERE 1 < \"USER_ID\"" + //
                "    GROUP BY \"USER_ID\"" + //
                "    HAVING 1 < COUNT(\"URL\")" + //
                "    ORDER BY \"USER_ID\" " + //
                ") LIMIT_SUBSELECT WHERE ROWNUM <= 10"; //
        final String actualSql = this.node.accept(this.generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    void testSqlGeneratorWithLimitOffset() throws AdapterException {
        ((SqlStatementSelect) this.node).getLimit().setOffset(5);
        final String expectedSql = "SELECT c0, c1 FROM (" + //
                "  SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( " + //
                "    SELECT \"USER_ID\" AS c0, COUNT(\"URL\") AS c1 " + //
                "      FROM \"SCHEMA\".\"CLICKS\"" + //
                "      WHERE 1 < \"USER_ID\"" + //
                "      GROUP BY \"USER_ID\"" + //
                "      HAVING 1 < COUNT(\"URL\")" + //
                "      ORDER BY \"USER_ID\"" + //
                "  ) LIMIT_SUBSELECT WHERE ROWNUM <= 15 " + //
                ") WHERE ROWNUM_SUB > 5";
        final String actualSql = this.node.accept(this.generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    void testSqlGeneratorWithSelectStarAndOffset() throws AdapterException {
        SqlStatementSelect node = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        node.getLimit().setOffset(5);
        node = new SqlStatementSelect(node.getFromClause(), SqlSelectList.createSelectStarSelectList(),
                node.getWhereClause(), node.getGroupBy(), node.getHaving(), node.getOrderBy(), node.getLimit());
        final String expectedSql = "SELECT c0, c1 FROM (" + //
                "  SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( " + //
                "    SELECT \"USER_ID\" AS c0, \"URL\" AS c1 " + //
                "      FROM \"SCHEMA\".\"CLICKS\"" + //
                "      WHERE 1 < \"USER_ID\"" + //
                "      GROUP BY \"USER_ID\"" + //
                "      HAVING 1 < COUNT(\"URL\")" + //
                "      ORDER BY \"USER_ID\"" + //
                "  ) LIMIT_SUBSELECT WHERE ROWNUM <= 15 " + //
                ") WHERE ROWNUM_SUB > 5";
        final String actualSql = node.accept(this.generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    void testMapColumnTypeWithMaximumDecimalPrecision() throws SQLException {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION;
        final int scale = 0;
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.dialect.dialectSpecificMapJdbcType(typeDescription),
                equalTo(DataType.createDecimal(precision, scale)));
    }

    private JdbcTypeDescription createTypeDescriptionForNumeric(final int precision, final int scale) {
        final int octetLength = 10;
        final JdbcTypeDescription typeDescription = new JdbcTypeDescription(Types.NUMERIC, scale, precision,
                octetLength, "NUMERIC");
        return typeDescription;
    }

    @ValueSource(strings = { "10,2", " 10,2", " 10 , 2 " })
    @ParameterizedTest
    void testMapColumnTypeBeyondMaximumDecimalPrecision(final String input) throws SQLException {
        final int precision = DataType.MAX_EXASOL_DECIMAL_PRECISION + 1;
        final int scale = 0;
        final int castPrecision = 10;
        final int castScale = 2;
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(OracleSqlDialect.ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY, castPrecision + "," + castScale);
        this.dialect = new OracleSqlDialect(null, new AdapterProperties(rawProperties));
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.dialect.dialectSpecificMapJdbcType(typeDescription),
                equalTo(DataType.createDecimal(castPrecision, castScale)));
    }

    @Test
    void testMapColumnTypeWithMagicScale() throws SQLException {
        final int precision = 10;
        final int scale = OracleSqlDialect.ORACLE_MAGIC_NUMBER_SCALE;
        final JdbcTypeDescription typeDescription = createTypeDescriptionForNumeric(precision, scale);
        assertThat(this.dialect.dialectSpecificMapJdbcType(typeDescription),
                equalTo(DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8)));
    }

    @CsvSource({ "FALSE, FALSE, JDBC", //
            "TRUE, FALSE, LOCAL", //
            "FALSE, TRUE, ORA" })
    @ParameterizedTest
    void testGetImportTypeLocal(final String local, final String fromOracle, final String expectedImportType) {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(OracleSqlDialect.LOCAL_IMPORT_PROPERTY, local);
        rawProperties.put(OracleSqlDialect.ORACLE_IMPORT_PROPERTY, fromOracle);
        final OracleSqlDialect dialect = new OracleSqlDialect(null, new AdapterProperties(rawProperties));
        assertThat(dialect.getImportType().toString(), equalTo(expectedImportType));
    }
}