package com.exasol.adapter.dialects.exasol;

import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;

@Tag("integration")
@Testcontainers
class ExasolSqlDialectIT {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtualschema-jdbc-adapter-dist-3.0.1.jar";
    private static final String SCHEMA_TEST = "SCHEMA_TEST";
    private static final String TABLE_ALL_EXASOL_DATA_TYPES = "TABLE_ALL_EXASOL_TYPES";
    private static final String TABLE_WITH_NULLS = "TABLE_WITH_NULLS";
    private static final String TABLE_SIMPLE_VALUES = "TABLE_SIMPLE_VALUES";
    private static final String VIRTUAL_SCHEMA_JDBC = "VIRTUAL_SCHEMA_JDBC";
    private static final String VIRTUAL_SCHEMA_JDBC_LOCAL = "VIRTUAL_SCHEMA_JDBC_LOCAL";
    private static final String VIRTUAL_SCHEMA_EXA = "VIRTUAL_SCHEMA_EXA";
    private static final String VIRTUAL_SCHEMA_EXA_LOCAL = "VIRTUAL_SCHEMA_EXA_LOCAL";

    private static final String SCHEMA_TEST_MIXED_CASE = "SCHEMA_TEST_Mixed_Case";
    private static final String TABLE_MIXED_CASE = "Table_Mixed_Case";
    private static final String VIRTUAL_SCHEMA_EXA_MIXED_CASE = "VIRTUAL_SCHEMA_EXA_Mixed_Case";

    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> container = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE).withClusterLogsPath(Path.of("target/mylogs"));
    private static Statement statement;

    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException {
        TimeUnit.SECONDS.sleep(10); // TODO need to be fixed in the test-containers
        final Bucket bucket = container.getDefaultBucket();
        final Path pathToRls = Path.of("target/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        bucket.uploadFile(pathToRls, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        final Connection connection = container.createConnectionForUser(container.getUsername(),
                container.getPassword());
        statement = connection.createStatement();
        createTestSchema(SCHEMA_TEST);
        createTestSchema("\"" + SCHEMA_TEST_MIXED_CASE + "\"");
        createTestTableAllExasolDataTypes();
        createTestTableWithNulls();
        createTestTableWithSimpleValues();
        createTestTableMixedCase();
        createConnection();
        createAdapterScript();
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC, SCHEMA_TEST, Optional.empty());
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC_LOCAL, SCHEMA_TEST, Optional.of("IS_LOCAL = 'true'"));
        createVirtualSchema(VIRTUAL_SCHEMA_EXA, SCHEMA_TEST,
                Optional.of("IMPORT_FROM_EXA = 'true' EXA_CONNECTION_STRING = 'localhost:8888'"));
        createVirtualSchema(VIRTUAL_SCHEMA_EXA_LOCAL, SCHEMA_TEST,
                Optional.of("IMPORT_FROM_EXA = 'true' EXA_CONNECTION_STRING = 'localhost:8888' IS_LOCAL = 'true'"));
        createVirtualSchema("\"" + VIRTUAL_SCHEMA_EXA_MIXED_CASE + "\"", SCHEMA_TEST_MIXED_CASE,
                Optional.of("IMPORT_FROM_EXA = 'true' EXA_CONNECTION_STRING = 'localhost:8888'"));
    }

    private static void createTestSchema(final String schemaName) throws SQLException {
        statement.execute("CREATE SCHEMA " + schemaName);
    }

    private static void createTestTableAllExasolDataTypes() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + SCHEMA_TEST + "." + TABLE_ALL_EXASOL_DATA_TYPES //
                + "(c1 VARCHAR(100) DEFAULT 'bar', " //
                + "c2 VARCHAR(100) CHARACTER SET ASCII DEFAULT 'bar', " //
                + "c3 CHAR(10) DEFAULT 'foo'," //
                + "c4 CHAR(10) CHARACTER SET ASCII DEFAULT 'bar', " //
                + "c5 DECIMAL(5,0) DEFAULT 1, " //
                + "c6 DECIMAL(6,3) DEFAULT 1.2, " //
                + "c7 DOUBLE DEFAULT 1E2, " //
                + "c8 BOOLEAN DEFAULT TRUE, " //
                + "c9 DATE DEFAULT '2016-06-01', " //
                + "c10 TIMESTAMP DEFAULT '2016-06-01 00:00:01.000', " //
                + "c11 TIMESTAMP WITH LOCAL TIME ZONE DEFAULT '2016-06-01 00:00:02.000', " //
                + "c12 INTERVAL YEAR TO MONTH DEFAULT '3-5', " //
                + "c13 INTERVAL DAY TO SECOND DEFAULT '2 12:50:10.123', " //
                + "c14 GEOMETRY(3857) DEFAULT 'POINT(2 5)' " //
                + ")");
        statement.execute("INSERT INTO " + SCHEMA_TEST + "." + TABLE_ALL_EXASOL_DATA_TYPES + " VALUES " //
                + "('a茶', 'b', 'c茶', 'd', 123, 123.456, 2.2, FALSE, '2016-08-01', '2016-08-01 00:00:01.000', " //
                + "'2016-08-01 00:00:02.000', '4-6', '3 12:50:10.123', 'POINT(2 5)')");
    }

    private static void createTestTableWithNulls() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + SCHEMA_TEST + "." + TABLE_WITH_NULLS //
                + "(c1 INT, " //
                + "c2 VARCHAR(100))");
        statement.execute("INSERT INTO " + SCHEMA_TEST + "." + TABLE_WITH_NULLS + " VALUES " //
                + "(1, 'a'), " //
                + "(2, null), " //
                + "(3, 'b'), " //
                + "(1, null), " //
                + "(null, 'c')");
    }

    private static void createTestTableWithSimpleValues() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + SCHEMA_TEST + "." + TABLE_SIMPLE_VALUES //
                + "(a INT, " //
                + "b VARCHAR(100), " //
                + "c DOUBLE)");
        statement.execute("INSERT INTO " + SCHEMA_TEST + "." + TABLE_SIMPLE_VALUES + " VALUES " //
                + "(1, 'a', 1.1), " //
                + "(2, 'b', 2.2), " //
                + "(3, 'c', 3.3), " //
                + "(1, 'd', 4.4), " //
                + "(2, 'e', 5.5), " //
                + "(3, 'f', 6.6), " //
                + "(null, null, null)");
    }

    private static void createTestTableMixedCase() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE \"" + SCHEMA_TEST_MIXED_CASE + "\".\"" + TABLE_MIXED_CASE + "\""//
                + "(\"Column1\" INT, " //
                + "\"column2\" INT, " //
                + "COLUMN3 INT)");
        statement.execute("INSERT INTO \"" + SCHEMA_TEST_MIXED_CASE + "\".\"" + TABLE_MIXED_CASE + "\" VALUES " //
                + "(1, 2, 3)");
    }

    private static void createConnection() throws SQLException {
        statement.execute("CREATE CONNECTION JDBC_EXASOL_CONNECTION " //
                + "TO 'jdbc:exa:localhost:8888' " //
                + "USER '" + container.getUsername() + "' " //
                + "IDENTIFIED BY '" + container.getPassword() + "'");
    }

    private static void createAdapterScript() throws SQLException, InterruptedException {
        statement.execute("CREATE OR REPLACE JAVA ADAPTER SCRIPT " + SCHEMA_TEST + ".ADAPTER_SCRIPT_EXASOL AS " //
                + "%scriptclass com.exasol.adapter.RequestDispatcher;\n" //
                + "%jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n" //
                + "/");
        TimeUnit.SECONDS.sleep(18); // FIXME: need to be fixed in the container
    }

    private static void createVirtualSchema(final String virtualSchemaName, final String schemaName,
            final Optional<String> additionalParameters) throws SQLException {
        statement.execute("OPEN SCHEMA " + "\"" + schemaName + "\"");
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE VIRTUAL SCHEMA ");
        builder.append(virtualSchemaName);
        builder.append(" USING " + SCHEMA_TEST + ".ADAPTER_SCRIPT_EXASOL WITH ");
        builder.append("SQL_DIALECT     = 'EXASOL' ");
        builder.append("CONNECTION_NAME = 'JDBC_EXASOL_CONNECTION' ");
        builder.append("SCHEMA_NAME     = '" + schemaName + "' ");
        builder.append("DEBUG_ADDRESS = '10.0.2.15:3000'");
        builder.append("LOG_LEVEL = 'ALL'");
        additionalParameters.ifPresent(builder::append);
        statement.execute(builder.toString());
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testDataTypeMapping(final String virtualSchemaName) throws SQLException {
        final String expectedSchemaQualifiedTableName = SCHEMA_TEST + ".EXA_DBA_COLUMNS_EXPECTED";
        statement.execute("CREATE OR REPLACE TABLE " + expectedSchemaQualifiedTableName //
                + "(COLUMN_NAME VARCHAR(128), " //
                + "COLUMN_TYPE VARCHAR(40), " //
                + "COLUMN_MAXSIZE DECIMAL(18,0), " //
                + "COLUMN_NUM_PREC DECIMAL(18, 0), " //
                + "COLUMN_NUM_SCALE DECIMAL(18, 0), " //
                + "COLUMN_DEFAULT VARCHAR(2000))");
        statement.execute("INSERT INTO " + expectedSchemaQualifiedTableName + " VALUES " //
                + "('C1', 'VARCHAR(100) UTF8', 100, NULL, NULL, '''bar'''), " //
                + "('C2', 'VARCHAR(100) ASCII', 100, NULL, NULL, '''bar'''), " //
                + "('C3', 'CHAR(10) UTF8', 10, NULL, NULL, '''foo'''), " //
                + "('C4', 'CHAR(10) ASCII', 10, NULL, NULL, '''bar'''), " //
                + "('C5', 'DECIMAL(5,0)', 5, 5, 0, 1), " //
                + "('C6', 'DECIMAL(6,3)', 6, 6, 3, 1.2), " //
                + "('C7', 'DOUBLE', 64, NULL, NULL, 100), " //
                + "('C8', 'BOOLEAN', 1, NULL, NULL, TRUE), " //
                + "('C9', 'DATE', 10, NULL, NULL, '''2016-06-01'''), " //
                + "('C10', 'TIMESTAMP', 29, NULL, NULL, '''2016-06-01 00:00:01.000'''), " //
                + "('C11', 'TIMESTAMP WITH LOCAL TIME ZONE', 29, NULL, NULL, '''2016-06-01 00:00:02.000'''), " //
                + "('C12', 'INTERVAL YEAR(2) TO MONTH', 13, NULL, NULL, '''3-5'''), " //
                + "('C13', 'INTERVAL DAY(2) TO SECOND(3)', 29, NULL, NULL, '''2 12:50:10.123'''), " //
                + "('C14', 'GEOMETRY(3857)', 8000000, NULL, NULL, '''POINT(2 5)''') " //
        );
        final ResultSet expected = statement.executeQuery("SELECT * FROM " + expectedSchemaQualifiedTableName);
        final ResultSet actual = statement
                .executeQuery("SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, "
                        + "COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '" + virtualSchemaName
                        + "' AND COLUMN_TABLE='" + TABLE_ALL_EXASOL_DATA_TYPES + "' ORDER BY COLUMN_ORDINAL_POSITION");
        assertThat(actual, matchesResultSet(expected));
    }

    private static Stream<String> getAllVirtualSchemaVariants() {
        return Stream.of(VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_JDBC_LOCAL, VIRTUAL_SCHEMA_EXA, VIRTUAL_SCHEMA_EXA_LOCAL);
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testVarchar(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C1, C2");
    }

    private void assertSelectFromColumnsResult(final String virtualSchemaName, final String columns)
            throws SQLException {
        final ResultSet expected = statement
                .executeQuery("SELECT " + columns + " FROM " + SCHEMA_TEST + "." + TABLE_ALL_EXASOL_DATA_TYPES);
        final ResultSet actual = statement
                .executeQuery("SELECT " + columns + " FROM " + virtualSchemaName + "." + TABLE_ALL_EXASOL_DATA_TYPES);
        assertThat(actual, matchesResultSet(expected));
    }

    // TODO: This test excludes VIRTUAL_SCHEMA_EXA virtual schema because of the wrong data mapping caused by driver.
    @ParameterizedTest
    @MethodSource("getVirtualSchemaVariantsWithoutExa")
    void testChar(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C3, C4");
    }

    private static Stream<String> getVirtualSchemaVariantsWithoutExa() {
        return Stream.of(VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_JDBC_LOCAL, VIRTUAL_SCHEMA_EXA_LOCAL);
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testDecimal(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C5, C6");
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testDouble(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C7");
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testBoolean(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C8");
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testDate(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C9");
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testTimestamp(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C10, C11");
    }

    // TODO: This test excludes VIRTUAL_SCHEMA_JDBC and VIRTUAL_SCHEMA_EXA virtual schema because of the bug on the
    // loader side.
    @ParameterizedTest
    @MethodSource("getVirtualSchemaVariantsOnlyLocal")
    void testIntervalYearToMonth(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C12");
    }

    private static Stream<String> getVirtualSchemaVariantsOnlyLocal() {
        return Stream.of(VIRTUAL_SCHEMA_JDBC_LOCAL, VIRTUAL_SCHEMA_EXA_LOCAL);
    }

    // TODO: This test excludes VIRTUAL_SCHEMA_JDBC and VIRTUAL_SCHEMA_EXA virtual schema because of the bug on the
    // loader side.
    @ParameterizedTest
    @MethodSource("getVirtualSchemaVariantsOnlyLocal")
    void testIntervalDayToSecond(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C13");
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testGeometry(final String virtualSchemaName) throws SQLException {
        assertSelectFromColumnsResult(virtualSchemaName, "C14");
    }

    @Test
    void testIdentifierCaseSensitivityOnTable() throws SQLException {
        final ResultSet expected = statement.executeQuery("SELECT \"Column1\", \"column2\", COLUMN3 FROM \""
                + SCHEMA_TEST_MIXED_CASE + "\".\"" + TABLE_MIXED_CASE + "\"");
        final ResultSet actual = statement.executeQuery("SELECT \"Column1\", \"column2\", COLUMN3 FROM \""
                + VIRTUAL_SCHEMA_EXA_MIXED_CASE + "\".\"" + TABLE_MIXED_CASE + "\"");
        assertThat(actual, matchesResultSet(expected));
    }

    @Test
    void assertUnquotedMixedCaseTableIsNotFound() {
        final SQLException exception = assertThrows(SQLException.class,
                () -> statement.executeQuery("SELECT * FROM \"" + SCHEMA_TEST_MIXED_CASE + "\"." + TABLE_MIXED_CASE));
        assertThat(exception.getMessage(),
                containsString("object \"SCHEMA_TEST_Mixed_Case\".\"TABLE_MIXED_CASE\" not found "));
    }

    @ParameterizedTest
    @ValueSource(strings = { "Column1", "column2" })
    void assertUnquotedMixedCaseColumnIsNotFound(final String columnName) throws SQLException {
        final SQLException exception = assertThrows(SQLException.class, () -> statement.executeQuery("SELECT "
                + columnName + " FROM \"" + VIRTUAL_SCHEMA_EXA_MIXED_CASE + "\".\"" + TABLE_MIXED_CASE + "\""));
        assertThat(exception.getMessage(), containsString("object " + columnName.toUpperCase() + " not found "));
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testGroupConcat(final String virtualSchemaName) throws SQLException {
        assertGroupConcatExpression(virtualSchemaName, "1,1,2,2,3,3", "GROUP_CONCAT(A)", "SELECT GROUP_CONCAT(\"");
    }

    private void assertGroupConcatExpression(final String virtualSchemaName, final String expectedResultString,
            final String groupConcatExpression, final String s3) throws SQLException {
        final String query = "SELECT " + groupConcatExpression + " FROM " + virtualSchemaName + "."
                + TABLE_SIMPLE_VALUES;
        final ResultSet result = statement.executeQuery(query);
        result.next();
        final String actualResultString = result.getString(1);
        final ResultSet explainVirtual = statement.executeQuery("EXPLAIN VIRTUAL " + query);
        explainVirtual.next();
        final String explainVirtualStringActual = explainVirtual.getString("PUSHDOWN_SQL");
        assertAll(
                () -> assertThat(explainVirtualStringActual,
                        containsString(s3 + TABLE_SIMPLE_VALUES + "\".\"A\") FROM \"" + SCHEMA_TEST + "\".\""
                                + TABLE_SIMPLE_VALUES + "\"")),
                () -> assertThat(actualResultString, containsString(expectedResultString)));
    }

    @ParameterizedTest
    @MethodSource("getAllVirtualSchemaVariants")
    void testGroupConcatDistinct(final String virtualSchemaName) throws SQLException {
        assertGroupConcatExpression(virtualSchemaName, "1,2,3", "GROUP_CONCAT(DISTINCT A)",
                "SELECT GROUP_CONCAT(DISTINCT \"");
    }
}