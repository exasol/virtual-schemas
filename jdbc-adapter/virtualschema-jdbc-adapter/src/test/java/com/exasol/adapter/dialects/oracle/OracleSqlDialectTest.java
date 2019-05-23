package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.IS_LOCAL_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.AVG;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.AVG_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.COUNT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.COUNT_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.COUNT_STAR;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.FIRST_VALUE;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.GROUP_CONCAT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.GROUP_CONCAT_ORDER_BY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.GROUP_CONCAT_SEPARATOR;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.LAST_VALUE;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.MAX;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.MEDIAN;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.MIN;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_POP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.STDDEV_SAMP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.SUM;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.SUM_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VARIANCE;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VARIANCE_DISTINCT;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_POP;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_SAMP;
import static com.exasol.adapter.capabilities.LiteralCapability.DATE;
import static com.exasol.adapter.capabilities.LiteralCapability.DOUBLE;
import static com.exasol.adapter.capabilities.LiteralCapability.EXACTNUMERIC;
import static com.exasol.adapter.capabilities.LiteralCapability.INTERVAL;
import static com.exasol.adapter.capabilities.LiteralCapability.NULL;
import static com.exasol.adapter.capabilities.LiteralCapability.STRING;
import static com.exasol.adapter.capabilities.LiteralCapability.TIMESTAMP;
import static com.exasol.adapter.capabilities.LiteralCapability.TIMESTAMP_UTC;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_COLUMN;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_EXPRESSION;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_TUPLE;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_HAVING;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_SINGLE_GROUP;
import static com.exasol.adapter.capabilities.MainCapability.FILTER_EXPRESSIONS;
import static com.exasol.adapter.capabilities.MainCapability.LIMIT;
import static com.exasol.adapter.capabilities.MainCapability.LIMIT_WITH_OFFSET;
import static com.exasol.adapter.capabilities.MainCapability.ORDER_BY_COLUMN;
import static com.exasol.adapter.capabilities.MainCapability.ORDER_BY_EXPRESSION;
import static com.exasol.adapter.capabilities.MainCapability.SELECTLIST_EXPRESSIONS;
import static com.exasol.adapter.capabilities.MainCapability.SELECTLIST_PROJECTION;
import static com.exasol.adapter.capabilities.PredicateCapability.AND;
import static com.exasol.adapter.capabilities.PredicateCapability.BETWEEN;
import static com.exasol.adapter.capabilities.PredicateCapability.EQUAL;
import static com.exasol.adapter.capabilities.PredicateCapability.IN_CONSTLIST;
import static com.exasol.adapter.capabilities.PredicateCapability.IS_NOT_NULL;
import static com.exasol.adapter.capabilities.PredicateCapability.IS_NULL;
import static com.exasol.adapter.capabilities.PredicateCapability.LESS;
import static com.exasol.adapter.capabilities.PredicateCapability.LESSEQUAL;
import static com.exasol.adapter.capabilities.PredicateCapability.LIKE;
import static com.exasol.adapter.capabilities.PredicateCapability.LIKE_ESCAPE;
import static com.exasol.adapter.capabilities.PredicateCapability.NOT;
import static com.exasol.adapter.capabilities.PredicateCapability.NOTEQUAL;
import static com.exasol.adapter.capabilities.PredicateCapability.OR;
import static com.exasol.adapter.capabilities.PredicateCapability.REGEXP_LIKE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ABS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ACOS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_DAYS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_HOURS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_MINUTES;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_MONTHS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_SECONDS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_WEEKS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ADD_YEARS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ASCII;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ASIN;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ATAN;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ATAN2;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.BIT_AND;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.BIT_TO_NUM;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.CASE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.CAST;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.CEIL;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.CHR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.COS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.COSH;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.COT;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.CURRENT_DATE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.CURRENT_TIMESTAMP;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.DBTIMEZONE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.DEGREES;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.DIV;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.EXP;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.FLOAT_DIV;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.FLOOR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.GREATEST;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.INSTR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LEAST;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LENGTH;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LN;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LOCALTIMESTAMP;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LOCATE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LOG;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LOWER;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LPAD;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.LTRIM;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.MOD;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.MULT;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.NEG;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.NULLIFZERO;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.NUMTODSINTERVAL;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.NUMTOYMINTERVAL;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.POWER;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.RADIANS;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.REGEXP_INSTR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.REGEXP_REPLACE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.REGEXP_SUBSTR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.REPEAT;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.REPLACE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.REVERSE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.RPAD;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.RTRIM;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SESSIONTIMEZONE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SIGN;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SIN;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SINH;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SOUNDEX;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SQRT;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SUB;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SUBSTR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SYSDATE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.SYSTIMESTAMP;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TAN;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TANH;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TO_CHAR;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TO_DATE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TO_DSINTERVAL;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TO_NUMBER;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TO_TIMESTAMP;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TO_YMINTERVAL;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TRANSLATE;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.TRIM;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.UPPER;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ZEROIFNULL;
import static com.exasol.adapter.dialects.oracle.OracleSqlDialect.ORACLE_IMPORT_PROPERTY;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.DialectTestData;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;

import utils.SqlTestUtil;

class OracleSqlDialectTest {
    private static final String SCHEMA_NAME = "SCHEMA";
    private SqlNode node;
    private SqlDialect dialect;
    private SqlGenerationVisitor generator;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.node = DialectTestData.getTestSqlNode();
        this.dialect = new OracleSqlDialect(null, AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("", SCHEMA_NAME, false);
        this.generator = this.dialect.getSqlGenerationVisitor(context);
        this.rawProperties = new HashMap<>();
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

    @CsvSource({ "FALSE, FALSE, JDBC", //
            "TRUE, FALSE, LOCAL", //
            "FALSE, TRUE, ORA" })
    @ParameterizedTest
    void testGetImportTypeLocal(final String local, final String fromOracle, final String expectedImportType) {
        this.rawProperties.put(IS_LOCAL_PROPERTY, local);
        this.rawProperties.put(ORACLE_IMPORT_PROPERTY, fromOracle);
        final OracleSqlDialect dialect = new OracleSqlDialect(null, new AdapterProperties(this.rawProperties));
        assertThat(dialect.getImportType().toString(), equalTo(expectedImportType));
    }

    @Test
    void testCheckOracleSpecificPropertyConsistencyInvalidDialect() {
        setMandatoryProperties("ORACLE");
        this.rawProperties.put("ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE", "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new OracleSqlDialect(null, adapterProperties);
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void testValidateCatalogProperty() {
        setMandatoryProperties("ORACLE");
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new OracleSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect ORACLE does not support CATALOG_NAME property. Please, do not set the CATALOG_NAME property."));
    }

    @Test
    void testValidateDialectNameProperty() {
        setMandatoryProperties("IMPALA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new OracleSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect ORACLE cannot have the name IMPALA. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties("ORACLE");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new OracleSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void setMandatoryProperties(final String sqlDialectProperty) {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, sqlDialectProperty);
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }

    @Test
    void testQueryRewriterClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createQueryRewriter"),
                instanceOf(OracleQueryRewriter.class));
    }
}