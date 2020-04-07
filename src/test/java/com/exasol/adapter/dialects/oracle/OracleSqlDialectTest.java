package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.dialects.oracle.OracleProperties.ORACLE_IMPORT_PROPERTY;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.*;
import com.exasol.sql.SqlNormalizer;

@ExtendWith(MockitoExtension.class)
class OracleSqlDialectTest {
    private static final String SCHEMA_NAME = "SCHEMA";
    private SqlNode node;
    private SqlDialect dialect;
    private SqlNodeVisitor<String> generator;
    @Mock
    private ConnectionFactory connectionFactoryMock;

    @BeforeEach
    void beforeEach() {
        this.node = DialectTestData.getTestSqlNode();
        this.dialect = new OracleSqlDialect(this.connectionFactoryMock, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("", SCHEMA_NAME, false);
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
                                LIMIT_WITH_OFFSET, JOIN, JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER,
                                JOIN_TYPE_FULL_OUTER, JOIN_CONDITION_EQUI)), //
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
        assertEquals(SqlNormalizer.normalizeSql(expectedSql), SqlNormalizer.normalizeSql(actualSql));
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
        assertEquals(SqlNormalizer.normalizeSql(expectedSql), SqlNormalizer.normalizeSql(actualSql));
    }

    @Test
    void testSqlGeneratorWithSelectStarAndOffset() throws AdapterException {
        SqlStatementSelect node = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        node.getLimit().setOffset(5);
        node = SqlStatementSelect.builder().selectList(SqlSelectList.createSelectStarSelectList())
                .fromClause(node.getFromClause()).whereClause(node.getWhereClause()).groupBy(node.getGroupBy())
                .having(node.getHaving()).orderBy(node.getOrderBy()).limit(node.getLimit()).build();
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
        assertEquals(SqlNormalizer.normalizeSql(expectedSql), SqlNormalizer.normalizeSql(actualSql));
    }

    @CsvSource({ "FALSE, FALSE, JDBC", //
            "TRUE, FALSE, LOCAL", //
            "FALSE, TRUE, ORA" })
    @ParameterizedTest
    void testGetImportTypeLocal(final String local, final String fromOracle, final String expectedImportType) {
        final OracleSqlDialect dialect = new OracleSqlDialect(null,
                new AdapterProperties(Map.of(IS_LOCAL_PROPERTY, local, //
                        ORACLE_IMPORT_PROPERTY, fromOracle)));
        assertThat(dialect.getImportType().toString(), equalTo(expectedImportType));
    }

    @Test
    void testCheckOracleSpecificPropertyConsistencyInvalidDialect() {
        final SqlDialect sqlDialect = new OracleSqlDialect(null,
                new AdapterProperties(Map.of(SQL_DIALECT_PROPERTY, "ORACLE", //
                        CONNECTION_NAME_PROPERTY, "MY_CONN", //
                        "ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE", "MY_CONN")));
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void testValidateCatalogProperty() {
        final SqlDialect sqlDialect = new OracleSqlDialect(null, new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "ORACLE", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                CATALOG_NAME_PROPERTY, "MY_CATALOG")));
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect ORACLE does not support CATALOG_NAME property. Please, do not set the CATALOG_NAME property."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "ORACLE", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                SCHEMA_NAME_PROPERTY, "MY_SCHEMA"));
        final SqlDialect sqlDialect = new OracleSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testQueryRewriterClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createQueryRewriter"),
                instanceOf(OracleQueryRewriter.class));
    }
}