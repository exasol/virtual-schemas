package com.exasol.adapter.dialects.mysql;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;
import static com.exasol.dbbuilder.dialects.exasol.AdapterScript.Language.JAVA;
import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import com.exasol.dbbuilder.dialects.Schema;
import com.exasol.dbbuilder.dialects.Table;
import com.exasol.dbbuilder.dialects.exasol.*;
import com.exasol.dbbuilder.dialects.mysql.MySqlObjectFactory;
import com.exasol.dbbuilder.dialects.mysql.MySqlSchema;

/**
 * How to run `MySqlSqlDialectIT`: See the documentation <a
 * href="doc/development/developing-sql-dialect/integration_testing_with_containers.md>integration_testing_with_containers.md</a>.
 */
@Tag("integration")
@Testcontainers
class MySqlSqlDialectIT extends AbstractIntegrationTest {
    private static final String MYSQL_DOCKER_IMAGE_REFERENCE = "mysql:8.0.20";
    private static final String RESOURCES_FOLDER_DIALECT_NAME = "mysql";
    private static final int MYSQL_PORT = 3306;
    private static final String JDBC_CONNECTION_NAME = "JDBC";
    private static final String MYSQL_SCHEMA = "MYSQL_SCHEMA";
    private static final String MYSQL_SIMPLE_TABLE = "MYSQL_SIMPLE_TABLE";
    private static final String MYSQL_NUMERIC_DATE_DATATYPES_TABLE = "MYSQL_NUMERIC_DATE_TABLE";
    private static final String MYSQL_STRING_DATATYPES_TABLE = "MYSQL_STRING_TABLE";
    private static final String VIRTUAL_SCHEMA_JDBC = "VIRTUAL_SCHEMA_JDBC";
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE);
    @Container
    private static final MySQLContainer<?> mySQLContainer = new MySQLContainer<>(MYSQL_DOCKER_IMAGE_REFERENCE)
            .withUsername("root").withPassword("");

    @BeforeAll
    static void beforeAll() throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final String driverName = getPropertyFromFile(RESOURCES_FOLDER_DIALECT_NAME, "driver.name");
        uploadDriverToBucket(driverName, RESOURCES_FOLDER_DIALECT_NAME, exasolContainer.getDefaultBucket());
        uploadVsJarToBucket(exasolContainer.getDefaultBucket());
        final MySqlObjectFactory mySqlFactory = new MySqlObjectFactory(mySQLContainer.createConnection(""));
        final MySqlSchema mySqlSchema = mySqlFactory.createSchema(MYSQL_SCHEMA);
        createMySqlSimpleTable(mySqlSchema);
        createMySqlNumericDateTable(mySqlSchema);
        createMySqlStringTable(mySqlSchema);
        createTestTablesForJoinTests(mySQLContainer.createConnection(""), mySqlSchema.getName());
        final ExasolObjectFactory exasolFactory = new ExasolObjectFactory(exasolContainer.createConnection(""));
        final ExasolSchema schema = exasolFactory.createSchema(SCHEMA_EXASOL);
        final String content = "%scriptclass com.exasol.adapter.RequestDispatcher;\n" //
                + "%jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n" //
                + "%jar /buckets/bfsdefault/default/drivers/jdbc/" + driverName + ";\n";
        final AdapterScript adapterScript = schema.createAdapterScript(ADAPTER_SCRIPT_EXASOL, JAVA, content);
        final String connectionString = "jdbc:mysql://" + DOCKER_IP_ADDRESS + ":"
                + mySQLContainer.getMappedPort(MYSQL_PORT) + "/";
        final ConnectionDefinition connectionDefinition = exasolFactory.createConnectionDefinition(JDBC_CONNECTION_NAME,
                connectionString, mySQLContainer.getUsername(), mySQLContainer.getPassword());
        exasolFactory.createVirtualSchemaBuilder(VIRTUAL_SCHEMA_JDBC).adapterScript(adapterScript)
                .connectionDefinition(connectionDefinition).dialectName("MYSQL")
                .properties(Map.of("CATALOG_NAME", MYSQL_SCHEMA)).build();
    }

    private static void createMySqlSimpleTable(final Schema mySqlSchema) {
        final Table table = mySqlSchema.createTable(MYSQL_SIMPLE_TABLE, List.of("int_col", "bool_col", "varchar_col"),
                List.of("INT", "BOOLEAN", "VARCHAR(20)"));
        table.insert(-100, true, "a");
        table.insert(-1, false, "abbb");
        table.insert(0, true, "b");
        table.insert(10, false, "bbbb");
        table.insert(50, true, "abc");
        table.insert(100, false, "a");
    }

    private static void createMySqlNumericDateTable(final Schema mySqlSchema) {
        final Table table = mySqlSchema.createTable(MYSQL_NUMERIC_DATE_DATATYPES_TABLE,
                List.of("BiT_Col", "tinyint_col", "BOOL_COL", "smallint_col", "mediumint_col", "int_col", "bigint_col",
                        "decimal_col", "float_col", "double_col", //
                        "date_col", "datetime_col", "timestamp_col", "time_col", "year_col"),
                List.of("BIT(6)", "TINYINT", "BOOLEAN", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "DECIMAL(5, 2)",
                        "FLOAT(7, 4)", "DOUBLE", //
                        "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR"));
        table.insert("5", 127, 1, 32767, 8388607, 2147483647, 9223372036854775807L, 999.99, 999.00009, 999.00009, //
                "1000-01-01", "1000-01-01 00:00:00", "1970-01-01 00:00:01.000000", "16:59:59.000000", 1901);
        table.insert("9", -127, 0, -32768, -8388608, -2147483648, -9223372036854775808L, -999.99, -999.9999, -999.9999, //
                "9999-12-31", "9999-12-31 22:59:59", "2037-01-19 03:14:07.999999", "05:34:13.000000", 2155);
        table.insert(null, 0, true, 0, 0, 0, 0, 0, 0, 0, //
                null, null, null, null, "1901");
        table.insert(null, 0, false, 0, 0, 0, 0, 0, 0, 0, //
                null, null, null, null, 69);
    }

    private static void createMySqlStringTable(final Schema mySqlSchema) {
        final Table table = mySqlSchema.createTable(MYSQL_STRING_DATATYPES_TABLE,
                List.of("binary_col", "varbinary_col", "tinyblob_col", "tinytext_col", "blob_col", "text_col",
                        "mediumblob_col", "mediumtext_col", "longblob_col", "longtext_col", "enum_col", "set_col",
                        "varchar_col", "char_col"),
                List.of(" BINARY(20)", "VARBINARY(20)", "TINYBLOB", "TINYTEXT", "BLOB", "TEXT", "MEDIUMBLOB",
                        "MEDIUMTEXT", "LONGBLOB", "LONGTEXT", "ENUM('1', '2', '3')", "SET('1')", "VARCHAR(16000)",
                        "CHAR(255)"));
        table.insert("a", "a", "a", "a", "blob", "text", "mediumblob", "mediumtext", "longblob", "longtext", "1", "1",
                "ab", "asd24");
        table.insert("a\0", "a\0", "aa", "b", "blob", "text2", "mediumblob2", "mediumtext2", "longblob", "longtext2",
                "2", "1", "a", "11111");
        table.insert(null, null, "aaa", "aaaaaaaaaaaaa", "bloooooooooooob", "text3", null, null, null, null, "3", null,
                "", "");
        table.insert(null, null, "aaaaa", "a", "blob", "text", null, null, null, null, null, null, null, null);
    }

    @Test
    void testSelectAll() throws SQLException {
        final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE;
        final ResultSet actualResultSet = getActualResultSet(query);
        final ResultSet expected = getExpectedResultSet(List.of("col1 INT", "col2 BOOLEAN", "col3 VARCHAR(20)"), //
                List.of("-100, true, 'a'", //
                        "-1, false, 'abbb'", //
                        "0, true, 'b'", //
                        "10, false, 'bbbb'", //
                        "50, true, 'abc'", //
                        "100, false, 'a'"));
        assertThat(actualResultSet, matchesResultSet(expected));
    }

    @Override
    protected Connection getExasolConnection() throws SQLException {
        return exasolContainer.createConnection("");
    }

    @Nested
    @DisplayName("Datatype tests")
    class DatatypeTest {
        @Test
        void testBit() throws SQLException {
            final String query = "SELECT \"BiT_Col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 BOOLEAN"), //
                    List.of("true", "true", "false", "false"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testTinyInt() throws SQLException {
            final String query = "SELECT \"tinyint_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DECIMAL(3,0)"), //
                    List.of("127", "-127", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testBoolean() throws SQLException {
            final String query = "SELECT \"BOOL_COL\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 BOOLEAN"), //
                    List.of("true", "false", "true", "false"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testSmallInt() throws SQLException {
            final String query = "SELECT \"smallint_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DECIMAL(5,0)"), //
                    List.of("32767", "-32768", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testMediumInt() throws SQLException {
            final String query = "SELECT \"mediumint_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DECIMAL(7,0)"), //
                    List.of("8388607", "-8388608", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testInt() throws SQLException {
            final String query = "SELECT \"int_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DECIMAL(10,0)"), //
                    List.of("2147483647", "-2147483648", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testBigInt() throws SQLException {
            final String query = "SELECT \"bigint_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DECIMAL(19,0)"), //
                    List.of("9223372036854775807", "-9223372036854775808", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testDecimal() throws SQLException {
            final String query = "SELECT \"decimal_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DECIMAL(5,2)"), //
                    List.of("999.99", "-999.99", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testFloat() throws SQLException {
            final String query = "SELECT \"float_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DOUBLE PRECISION"), //
                    List.of("999.0001", "-999.9999", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testDouble() throws SQLException {
            final String query = "SELECT \"double_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DOUBLE PRECISION"), //
                    List.of("999.00009", "-999.9999", "0", "0"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testDate() throws SQLException {
            final String query = "SELECT \"date_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DATE"), //
                    List.of("'1000-01-01'", "'9999-12-31'", "null", "null"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testDatetime() throws SQLException {
            final String query = "SELECT \"datetime_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 TIMESTAMP"), //
                    List.of("'1000-01-01 01:00:00'", "'9999-12-31 23:59:59'", "null", "null"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testTimestamp() throws SQLException {
            final String query = "SELECT \"timestamp_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 TIMESTAMP"), //
                    List.of("'1970-01-01 01:00:01.000000'", "'2037-01-19 04:14:08.0'", "null", "null"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testTime() throws SQLException {
            final String query = "SELECT \"time_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 TIMESTAMP"), //
                    List.of("'1970-01-01 17:59:59.000000'", "'1970-01-01 06:34:13.000000'", "null", "null"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testYear() throws SQLException {
            final String query = "SELECT \"year_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_NUMERIC_DATE_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 DATE"), //
                    List.of("'1901-01-01'", "'2155-01-01'", "'1901-01-01'", "'2069-01-01'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        // Unsupported data types: BINARY, BLOB, LONGBLOB, MEDIUMBLOB, TINYBLOB, VARBINARY.
        void testUnsupported() throws SQLException, InterruptedException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_STRING_DATATYPES_TABLE;
            final SQLException exception = assertThrows(SQLException.class, () -> getActualResultSet(query));
            assertThat(exception.getMessage(),
                    containsString(" Unsupported data type(s) in column(s) 1, 2, 3, 5, 7, 9 in query."));
        }

        @Test
        void testTinyText() throws SQLException {
            final String query = "SELECT \"tinytext_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 VARCHAR(100)"), //
                    List.of("'a'", "'b'", "'aaaaaaaaaaaaa'", "'a'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testText() throws SQLException {
            final String query = "SELECT \"text_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 VARCHAR(65535)"), //
                    List.of("'text'", "'text2'", "'text3'", "'text'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testMediumText() throws SQLException {
            final String query = "SELECT \"mediumtext_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 VARCHAR(2000000)"), //
                    List.of("'mediumtext'", "'mediumtext2'", "NULL", "NULL"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testLongText() throws SQLException {
            final String query = "SELECT \"longtext_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 VARCHAR(2000000)"), //
                    List.of("'longtext'", "'longtext2'", "NULL", "NULL"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testEnum() throws SQLException {
            final String query = "SELECT \"enum_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 CHAR"), //
                    List.of("'1'", "'2'", "'3'", "NULL"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testSet() throws SQLException {
            final String query = "SELECT \"set_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 CHAR"), //
                    List.of("'1'", "'1'", "NULL", "NULL"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testVarchar() throws SQLException {
            final String query = "SELECT \"varchar_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "."
                    + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 VARCHAR(100)"), //
                    List.of("'ab'", "'a'", "''", "NULL"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testChar() throws SQLException {
            final String query = "SELECT \"char_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_STRING_DATATYPES_TABLE;
            final ResultSet expected = getExpectedResultSet(List.of("col1 CHAR(255)"), //
                    List.of("'asd24'", "'11111'", "''", "NULL"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }
    }

    @Nested
    @DisplayName("Join test")
    class JoinTest {
        @Test
        void testInnerJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " a INNER JOIN  "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_2 + " b ON a.\"x\"=b.\"x\"";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("2,'bbb', 2,'bbb'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testInnerJoinWithProjection() throws SQLException {
            final String query = "SELECT b.\"y\" || " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + ".\"y\" FROM "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " INNER JOIN  " + VIRTUAL_SCHEMA_JDBC + "."
                    + TABLE_JOIN_2 + " b ON " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + ".\"x\"=b.\"x\"";
            final ResultSet expected = getExpectedResultSet(List.of("y VARCHAR(100)"), //
                    List.of("'bbbbbb'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testLeftJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " a LEFT OUTER JOIN "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_2 + " b ON a.\"x\"=b.\"x\" ORDER BY a.\"x\"";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("1, 'aaa', null, null", //
                            "2, 'bbb', 2, 'bbb'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testRightJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " a RIGHT OUTER JOIN "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_2 + " b ON a.\"x\"=b.\"x\" ORDER BY a.\"x\"";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " a FULL OUTER JOIN "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_2 + " b ON a.\"x\"=b.\"x\" ORDER BY a.\"x\"";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("1, 'aaa', null, null", //
                            "2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testRightJoinWithComplexCondition() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " a RIGHT OUTER JOIN "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_2
                    + " b ON a.\"x\"||a.\"y\"=b.\"x\"||b.\"y\" ORDER BY a.\"x\"";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoinWithComplexCondition() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_1 + " a FULL OUTER JOIN "
                    + VIRTUAL_SCHEMA_JDBC + "." + TABLE_JOIN_2 + " b ON a.\"x\"-b.\"x\"=0 ORDER BY a.\"x\"";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("1, 'aaa', null, null", //
                            "2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }
    }

    @Test
    void testAggregateGroupByColumn() throws SQLException {
        final String query = "SELECT \"bool_col\", min(\"int_col\") FROM " + VIRTUAL_SCHEMA_JDBC + "."
                + MYSQL_SIMPLE_TABLE + " GROUP BY \"bool_col\"";
        final ResultSet expected = getExpectedResultSet(List.of("bool_col BOOLEAN", "int_col DECIMAL(10,0)"),
                List.of("true, -100", //
                        "false, -1"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testAggregateHaving() throws SQLException {
        final String query = "SELECT \"bool_col\", min(\"int_col\") FROM " + VIRTUAL_SCHEMA_JDBC + "."
                + MYSQL_SIMPLE_TABLE + " GROUP BY \"bool_col\" HAVING MIN(\"int_col\") < 0";
        final ResultSet expected = getExpectedResultSet(List.of("bool_col BOOLEAN", "int_col DECIMAL(10,0)"),
                List.of("true, -100", //
                        "false, -1"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    // =, !=, <, <=, >, >=
    void testComparisonPredicates() throws SQLException {
        final String query = "SELECT \"int_col\", \"int_col\" = 60, \"int_col\" != 60, \"int_col\" < 60, "
                + "\"int_col\" <= 60, \"int_col\" > 60, \"int_col\" >= 60 FROM " + VIRTUAL_SCHEMA_JDBC + "."
                + MYSQL_SIMPLE_TABLE + " WHERE \"int_col\" = 0";
        final ResultSet expected = getExpectedResultSet(
                List.of("int_col DECIMAL(10,0)", "b1 DECIMAL(19,0)", "b2 DECIMAL(19,0)", "b3 DECIMAL(19,0)",
                        "b4 DECIMAL(19,0)", "b5 DECIMAL(19,0)", "b6 DECIMAL(19,0)"), //
                List.of("0, 0, 1, 1, 1, 0, 0"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    // NOT, AND, OR
    void testLogicalPredicates() throws SQLException {
        final String query = "SELECT \"int_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE
                + " WHERE (\"int_col\" < 0 or \"int_col\" > 0) AND NOT (\"int_col\" is null)";
        final ResultSet expected = getExpectedResultSet(List.of("int_col DECIMAL(10,0)"), //
                List.of("-100", //
                        "-1", //
                        "10", //
                        "50", //
                        "100"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    // LIKE, LIKE ESCAPE (not pushed down)
    void testLikePredicates() throws SQLException {
        final String query = "SELECT \"varchar_col\", \"varchar_col\" LIKE 'a%' ESCAPE 'a' FROM " + VIRTUAL_SCHEMA_JDBC
                + "." + MYSQL_SIMPLE_TABLE + " WHERE (\"varchar_col\" LIKE 'a%')";
        final ResultSet expected = getExpectedResultSet(List.of("varchar_col VARCHAR(10)", "bool_col BOOLEAN"),
                List.of("'a', false", //
                        "'abbb', false", //
                        "'abc', false", //
                        "'a', false"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    // BETWEEN, IN, IS NULL, !=NULL(rewritten to "IS NOT NULL")
    void testMiscPredicates() throws SQLException {
        final String query = "SELECT \"int_col\", \"int_col\" in (56, 61), \"int_col\" is null, \"int_col\" != null"
                + " FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE + " WHERE \"int_col\" between -10 and 51";
        final ResultSet expected = getExpectedResultSet(
                List.of("int_col DECIMAL(10,0)", "b1 BOOLEAN", "b2 BOOLEAN", "b3 BOOLEAN"), //
                List.of("-1, false, false, false", //
                        "0, false, false, false", //
                        "10, false, false, false", //
                        "50, false, false, false"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testCountSumAggregateFunction() throws SQLException {
        final String query = "SELECT COUNT(\"int_col\"), COUNT(*), COUNT(DISTINCT \"int_col\"), SUM(\"int_col\"), "
                + "SUM(DISTINCT \"int_col\") FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE;
        final ResultSet expected = getExpectedResultSet(
                List.of("a DECIMAL(10,0)", "b DECIMAL(10,0)", "c DECIMAL(10,0)", "d DECIMAL(19,0)", "e DECIMAL(19,0)"),
                List.of("6, 6, 6, 59, 59"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testAvgMinMaxAggregateFunction() throws SQLException {
        final String query = "SELECT AVG(\"int_col\"), MIN(\"int_col\"), MAX(\"int_col\") FROM " + VIRTUAL_SCHEMA_JDBC
                + "." + MYSQL_SIMPLE_TABLE;
        final ResultSet expected = getExpectedResultSet(
                List.of("a DECIMAL(14,4)", "b DECIMAL(10,0)", "c DECIMAL(10,0)"), //
                List.of("9.8333, -100.0000, 100.0000"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testCastedStringFunctions() throws SQLException {
        final String query = "SELECT concat(upper(\"varchar_col\"),lower(repeat(\"varchar_col\",2))) FROM "
                + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE;
        final ResultSet expected = getExpectedResultSet(List.of("a VARCHAR(100)"), //
                List.of("'Aaa'", //
                        "'ABBBabbbabbb'", //
                        "'Bbb'", //
                        "'BBBBbbbbbbbb'", //
                        "'ABCabcabc'", //
                        "'Aaa'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testRewrittenDivAndModFunctions() throws SQLException {
        final String query = "SELECT DIV(\"int_col\",\"int_col\"), mod(\"int_col\",\"int_col\") FROM "
                + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE;
        final ResultSet expected = getExpectedResultSet(List.of("a DECIMAL(19,0)", "b DECIMAL(19,0)"), //
                List.of("1, 0", //
                        "1, 0", //
                        "null, null", //
                        "1, 0", //
                        "1, 0", //
                        "1, 0"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testRewrittenSubStringFunction() throws SQLException {
        final String query = "SELECT substring(\"varchar_col\" FROM 1 FOR 2) FROM " + VIRTUAL_SCHEMA_JDBC + "."
                + MYSQL_SIMPLE_TABLE;
        final ResultSet expected = getExpectedResultSet(List.of("a VARCHAR(100)"), //
                List.of("'a'", //
                        "'ab'", //
                        "'b'", //
                        "'bb'", //
                        "'ab'", //
                        "'a'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    public void testOrderByLimit() throws SQLException {
        final String query = "SELECT \"bool_col\", \"int_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE
                + " ORDER BY \"int_col\" LIMIT 3";
        final ResultSet expected = getExpectedResultSet(List.of("a BOOLEAN", "b DECIMAL(10,0)"), //
                List.of("true, -100", //
                        "false, -1", //
                        "true, 0"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    public void testOrderByLimitOffset() throws SQLException {
        final String query = "SELECT \"bool_col\", \"int_col\" FROM " + VIRTUAL_SCHEMA_JDBC + "." + MYSQL_SIMPLE_TABLE
                + " ORDER BY \"int_col\" LIMIT 2 OFFSET 1";
        final ResultSet expected = getExpectedResultSet(List.of("a BOOLEAN", "b DECIMAL(10,0)"), //
                List.of("false, -1", //
                        "true, 0"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }
}