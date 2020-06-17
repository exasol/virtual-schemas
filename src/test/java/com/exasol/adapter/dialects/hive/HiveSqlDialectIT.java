package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;
import static com.exasol.dbbuilder.dialects.exasol.AdapterScript.Language.JAVA;
import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.io.File;
import java.math.BigDecimal;
import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import com.exasol.dbbuilder.dialects.exasol.*;

/**
 * How to run `HiveSqlDialectIT`: See the documentation <a
 * href="doc/development/developing-sql-dialect/integration_testing_with_containers.md>integration_testing_with_containers.md</a>.
 */
@Tag("integration")
@Testcontainers
class HiveSqlDialectIT extends AbstractIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSqlDialectIT.class);
    public static final String HIVE_DOCKER_COMPOSE_YAML = "src/test/resources/integration/driver/hive/docker-compose.yaml";
    private static final String HIVE_SERVICE_NAME = "hive-server_1";
    private static final String RESOURCES_FOLDER_DIALECT_NAME = "hive";
    private static final int HIVE_EXPOSED_PORT = 10000;
    private static final String JDBC_CONNECTION_NAME = "JDBC";
    private static final String SCHEMA_HIVE = "default";
    private static final String HIVE_USERNAME = "hive";
    private static final String HIVE_PASSWORD = "hive";
    private static final String TABLE_HIVE_SIMPLE = "TABLE_HIVE_SIMPLE";
    private static final String TABLE_HIVE_DECIMAL_CAST = "TABLE_HIVE_DECIMAL_CAST";
    private static final String TABLE_HIVE_ALL_DATA_TYPES = "TABLE_HIVE_ALL_DATA_TYPES";
    private static final String VIRTUAL_SCHEMA_HIVE_JDBC = "VIRTUAL_SCHEMA_HIVE_JDBC";
    private static final String VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL = "VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL";
    @Container
    public static DockerComposeContainer<? extends DockerComposeContainer<?>> hiveContainer = new DockerComposeContainer<>(
            new File(HIVE_DOCKER_COMPOSE_YAML)) //
                    .withExposedService(HIVE_SERVICE_NAME, HIVE_EXPOSED_PORT,
                            Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(50)));
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER)); //
    private static Statement statementExasol;

    @BeforeAll
    static void beforeAll()
            throws InterruptedException, BucketAccessException, TimeoutException, SQLException, ClassNotFoundException {
        final String driverName = getPropertyFromFile(RESOURCES_FOLDER_DIALECT_NAME, "driver.name");
        uploadDriverToBucket(driverName, RESOURCES_FOLDER_DIALECT_NAME, exasolContainer.getDefaultBucket());
        uploadVsJarToBucket(exasolContainer.getDefaultBucket());
        final Connection exasolConnection = exasolContainer.createConnectionForUser(exasolContainer.getUsername(),
                exasolContainer.getPassword());
        statementExasol = exasolConnection.createStatement();
        try (final Connection connection = getHiveConnection()) {
            final Statement statementHive = connection.createStatement();
            createTableHiveSimple(statementHive);
            createTableDecimalCast(statementHive);
            createTableAllDataTypes(statementHive);
            createTestTablesForJoinTests(connection, SCHEMA_HIVE);
        }
        final ExasolObjectFactory exasolFactory = new ExasolObjectFactory(exasolContainer.createConnection(""));
        final ExasolSchema schema = exasolFactory.createSchema(SCHEMA_EXASOL);
        final String content = "%scriptclass com.exasol.adapter.RequestDispatcher;\n" //
                + "%jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n" //
                + "%jar /buckets/bfsdefault/default/drivers/jdbc/" + driverName + ";\n";
        final AdapterScript adapterScript = schema.createAdapterScript(ADAPTER_SCRIPT_EXASOL, JAVA, content);
        final String connectionString = "jdbc:hive2://" + DOCKER_IP_ADDRESS + ":" + HIVE_EXPOSED_PORT + "/"
                + SCHEMA_HIVE;
        final ConnectionDefinition connectionDefinition = exasolFactory.createConnectionDefinition(JDBC_CONNECTION_NAME,
                connectionString, HIVE_USERNAME, HIVE_PASSWORD);
        exasolFactory.createVirtualSchemaBuilder(VIRTUAL_SCHEMA_HIVE_JDBC).adapterScript(adapterScript)
                .connectionDefinition(connectionDefinition).dialectName("HIVE")
                .properties(Map.of("SCHEMA_NAME", SCHEMA_HIVE)).build();
        exasolFactory.createVirtualSchemaBuilder(VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL)
                .adapterScript(adapterScript).connectionDefinition(connectionDefinition).dialectName("HIVE")
                .properties(Map.of("SCHEMA_NAME", SCHEMA_HIVE, "hive_cast_number_to_decimal_with_precision_and_scale",
                        "36,2"))
                .build();
    }

    private static Connection getHiveConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        return DriverManager.getConnection("jdbc:hive2://localhost:" + HIVE_EXPOSED_PORT + "/" + SCHEMA_HIVE,
                HIVE_USERNAME, HIVE_PASSWORD);
    }

    private static void createTableHiveSimple(final Statement statementHive) throws SQLException {
        statementHive.execute("CREATE TABLE " + TABLE_HIVE_SIMPLE + "(X INT)");
        statementHive.execute("TRUNCATE TABLE " + TABLE_HIVE_SIMPLE);
        statementHive.execute("INSERT INTO " + TABLE_HIVE_SIMPLE + " VALUES (99)");
    }

    private static void createTableDecimalCast(final Statement statementHive) throws SQLException {
        statementHive.execute("CREATE TABLE " + TABLE_HIVE_DECIMAL_CAST
                + "(DECIMAL_COL1 DECIMAL(12, 6), DECIMAL_COL2 DECIMAL(36, 16), DECIMAL_COL3 DECIMAL(38, 17))");
        statementHive.execute("TRUNCATE TABLE " + TABLE_HIVE_DECIMAL_CAST);
        statementHive.execute("INSERT INTO " + TABLE_HIVE_DECIMAL_CAST
                + " VALUES (123456.12345671, 123456789.011111111111111, 1234444444444444444.5555555555555555555555550)");

    }

    private static void createTableAllDataTypes(final Statement statementHive) throws SQLException {
        statementHive.execute("CREATE TABLE " + TABLE_HIVE_ALL_DATA_TYPES + //
                "(arraycol ARRAY<string>, " //
                + "biginteger BIGINT, " //
                + "boolcolumn BOOLEAN, " //
                + "charcolumn CHAR(1), " //
                + "decimalcol DECIMAL(10,0), " //
                + "doublecol DOUBLE, " //
                + "floatcol FLOAT, " //
                + "intcol INT, " //
                + "mapcol MAP<string,int>, " //
                + "smallinteger SMALLINT, " //
                + "stringcol STRING, " //
                + "structcol struct<a : int, b : int>, " //
                + "timestampcol TIMESTAMP, " //
                + "tinyinteger TINYINT, " //
                + "varcharcol VARCHAR(10), " //
                + "binarycol BINARY, " //
                + "datecol DATE)");
        statementHive.execute("TRUNCATE TABLE " + TABLE_HIVE_ALL_DATA_TYPES);
        statementHive.execute("INSERT INTO " + TABLE_HIVE_ALL_DATA_TYPES
                + "(arraycol,biginteger,boolcolumn,charcolumn,decimalcol,doublecol,floatcol,intcol,mapcol,smallinteger,"
                + "stringcol,structcol,timestampcol,tinyinteger,varcharcol,binarycol,datecol) SELECT " //
                + "ARRAY('etet','ettee'), " //
                + "56, " //
                + "true, " //
                + "'2', " //
                + "53, " //
                + "56.3, " //
                + "5.199999809265137, " //
                + "85, " //
                + "map('jkljj',5), " //
                + "2, " //
                + "'tshg', " //
                + "named_struct('a',2,'b',4), " //
                + "timestamp '2017-01-02 13:32:50.744', " //
                + "1, " //
                + "'tytu', " //
                + "'MTAxMA==', " //
                + "date '1970-01-01' " //
                + "FROM " + TABLE_HIVE_SIMPLE);
    }

    @Test
    public void testSetup() throws SQLException {
        final String query = "SELECT X FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_SIMPLE;
        assertExpressionExecutionLongResult(query, 99L);
    }

    private void assertExpressionExecutionLongResult(final String query, final long expected) throws SQLException {
        final ResultSet result = statementExasol.executeQuery(query);
        result.next();
        final long actualResult = result.getLong(1);
        assertThat(actualResult, equalTo(expected));
    }

    @Override
    protected Connection getExasolConnection() throws SQLException {
        return exasolContainer.createConnection("");
    }

    @Nested
    @DisplayName("Join test")
    class JoinTest {
        @Test
        void testInnerJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a INNER JOIN  "
                    + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x=b.x";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("2,'bbb', 2,'bbb'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testInnerJoinWithProjection() throws SQLException {
            final String query = "SELECT b.y || " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + ".y FROM "
                    + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " INNER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC + "."
                    + TABLE_JOIN_2 + " b ON " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + ".x=b.x";
            final ResultSet expected = getExpectedResultSet(List.of("y VARCHAR(100)"), //
                    List.of("'bbbbbb'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testLeftJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1
                    + " a LEFT OUTER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2
                    + " b ON a.x=b.x ORDER BY a.x";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("1, 'aaa', null, null", //
                            "2, 'bbb', 2, 'bbb'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testRightJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1
                    + " a RIGHT OUTER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2
                    + " b ON a.x=b.x ORDER BY a.x";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoin() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1
                    + " a FULL OUTER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2
                    + " b ON a.x=b.x ORDER BY a.x";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("1, 'aaa', null, null", //
                            "2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testRightJoinWithComplexCondition() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1
                    + " a RIGHT OUTER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2
                    + " b ON a.x||a.y=b.x||b.y ORDER BY a.x";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoinWithComplexCondition() throws SQLException {
            final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1
                    + " a FULL OUTER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2
                    + " b ON a.x-b.x=0 ORDER BY a.x";
            final ResultSet expected = getExpectedResultSet(
                    List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                    List.of("1, 'aaa', null, null", //
                            "2, 'bbb', 2, 'bbb'", //
                            "null, null, 3, 'ccc'"));
            assertThat(getActualResultSet(query), matchesResultSet(expected));
        }
    }

    @Test
    void testDataTypeMapping() throws SQLException {
        final String expectedSchemaQualifiedTableName = SCHEMA_EXASOL + ".EXPECTED";
        statementExasol.execute("CREATE OR REPLACE TABLE " + expectedSchemaQualifiedTableName //
                + "(COLUMN_NAME VARCHAR(128), " //
                + "COLUMN_TYPE VARCHAR(40), " //
                + "COLUMN_MAXSIZE DECIMAL(18,0), " //
                + "COLUMN_NUM_PREC DECIMAL(18, 0), " //
                + "COLUMN_NUM_SCALE DECIMAL(18, 0), " //
                + "COLUMN_DEFAULT VARCHAR(2000))");
        statementExasol.execute("INSERT INTO " + expectedSchemaQualifiedTableName + " VALUES " //
                + "('ARRAYCOL', 'VARCHAR(255) ASCII', 255, NULL, NULL, NULL), " //
                + "('BIGINTEGER', 'DECIMAL(19,0)', 19, 19, 0, NULL), " //
                + "('BOOLCOLUMN', 'BOOLEAN', 1, NULL, NULL, NULL), " //
                + "('CHARCOLUMN', 'CHAR(1) ASCII', 1, NULL, NULL, NULL), " //
                + "('DECIMALCOL', 'DECIMAL(10,0)', 10, 10, 0, NULL), " //
                + "('DOUBLECOL', 'DOUBLE', 64, NULL, NULL, NULL), " //
                + "('FLOATCOL', 'DOUBLE', 64, NULL, NULL, NULL), " //
                + "('INTCOL', 'DECIMAL(10,0)', 10, 10, 0, NULL), " //
                + "('MAPCOL', 'VARCHAR(255) ASCII', 255, NULL, NULL, NULL), " //
                + "('SMALLINTEGER', 'DECIMAL(5,0)', 5, 5, 0, NULL), " //
                + "('STRINGCOL', 'VARCHAR(255) ASCII', 255, NULL, NULL, NULL), " //
                + "('STRUCTCOL', 'VARCHAR(255) ASCII', 255, NULL, NULL, NULL), " //
                + "('TIMESTAMPCOL', 'TIMESTAMP', 29, NULL, NULL, NULL), " //
                + "('TINYINTEGER', 'DECIMAL(3,0)', 3, 3, 0, NULL), " //
                + "('VARCHARCOL', 'VARCHAR(10) ASCII', 10, NULL, NULL, NULL), " //
                + "('BINARYCOL', 'VARCHAR(2000000) UTF8', 2000000, NULL, NULL, NULL), " //
                + "('DATECOL', 'DATE', 10, NULL, NULL, NULL) " //
        );
        final ResultSet expected = statementExasol.executeQuery("SELECT * FROM " + expectedSchemaQualifiedTableName);
        final ResultSet actual = statementExasol
                .executeQuery("SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, "
                        + "COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '" + VIRTUAL_SCHEMA_HIVE_JDBC
                        + "' AND COLUMN_TABLE='" + TABLE_HIVE_ALL_DATA_TYPES + "' ORDER BY COLUMN_ORDINAL_POSITION");
        assertThat(actual, matchesResultSet(expected));
    }

    @Test
    void testSelectWithAllTypes() throws SQLException {
        final String expectedSchemaQualifiedTableName = SCHEMA_EXASOL + ".EXPECTED";
        statementExasol.execute("CREATE OR REPLACE TABLE " + expectedSchemaQualifiedTableName + //
                "(arraycol VARCHAR(255) ASCII, " //
                + "biginteger DECIMAL(19,0), " //
                + "boolcolumn BOOLEAN, " //
                + "charcolumn CHAR(1) ASCII, " //
                + "decimalcol DECIMAL(10,0), " //
                + "doublecol DOUBLE, " //
                + "floatcol DOUBLE, " //
                + "intcol DECIMAL(10,0), " //
                + "mapcol VARCHAR(255) ASCII, " //
                + "smallinteger DECIMAL(5,0), " //
                + "stringcol VARCHAR(255) ASCII, " //
                + "structcol VARCHAR(255) ASCII, " //
                + "timestampcol TIMESTAMP, " //
                + "tinyinteger DECIMAL(3,0), " //
                + "varcharcol VARCHAR(10) ASCII, " //
                + "binarycol VARCHAR(2000000) UTF8, " //
                + "datecol DATE)");
        statementExasol.execute("INSERT INTO " + expectedSchemaQualifiedTableName + " VALUES " //
                + "('[\"etet\",\"ettee\"]', " //
                + "56, " //
                + "true, " //
                + "2, " //
                + "53, " //
                + "56.3, " //
                + "5.199999809265137, " //
                + "85, " //
                + "'{\"jkljj\":5}', " //
                + "2, " //
                + "'tshg', " //
                + "'{\"a\":2,\"b\":4}', " //
                + "'2017-01-02 13:32:50.744', " //
                + "1, " //
                + "'tytu', " //
                + "'TVRBeE1BPT0=', " //
                + "'1970-01-01' " //
                + ")");
        final ResultSet expected = statementExasol.executeQuery("SELECT * FROM " + expectedSchemaQualifiedTableName);
        final ResultSet actual = statementExasol
                .executeQuery("SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES);
        assertThat(actual, matchesResultSet(expected));
    }

    @Test
    void testProjection() {
        final String qualifiedTableName = VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String query = "SELECT BIGINTEGER FROM " + qualifiedTableName;
        assertAll(() -> assertExpressionExecutionBigDecimalResult(query, new BigDecimal("56")),
                () -> assertExplainVirtual(query, "SELECT `" + TABLE_HIVE_ALL_DATA_TYPES + "`.`BIGINTEGER` FROM `"
                        + SCHEMA_HIVE + "`.`" + TABLE_HIVE_ALL_DATA_TYPES + "`"));
    }

    private void assertExpressionExecutionBigDecimalResult(final String query, final BigDecimal expectedValue)
            throws SQLException {
        final ResultSet result = statementExasol.executeQuery(query);
        result.next();
        final BigDecimal actualResult = result.getBigDecimal(1);
        assertThat(actualResult.stripTrailingZeros(), equalTo(expectedValue));
    }

    private void assertExplainVirtual(final String query, final String expected) throws SQLException {
        final ResultSet explainVirtual = statementExasol.executeQuery("EXPLAIN VIRTUAL " + query);
        explainVirtual.next();
        final String explainVirtualStringActual = explainVirtual.getString("PUSHDOWN_SQL");
        assertThat(explainVirtualStringActual, containsString(expected));
    }

    @Test
    void testRewrittenProjection() {
        final String qualifiedTableName = VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String query = "SELECT BINARYCOL FROM " + qualifiedTableName;
        final String expectedExplainVirtual = "SELECT base64(`" + TABLE_HIVE_ALL_DATA_TYPES + "`.`BINARYCOL`) FROM `"
                + SCHEMA_HIVE + "`.`" + TABLE_HIVE_ALL_DATA_TYPES;
        assertAll(() -> assertExpressionExecutionStringResult(query, "TVRBeE1BPT0="),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    private void assertExpressionExecutionStringResult(final String query, final String expected) throws SQLException {
        final ResultSet result = statementExasol.executeQuery(query);
        result.next();
        final String actual = result.getString(1);
        assertThat(actual, containsString(expected));
    }

    @Test
    void testAggregateGroupByColumn() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a BOOLEAN", "b DECIMAL(19,0)"), //
                List.of("true, 56"));
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "."
                + TABLE_HIVE_ALL_DATA_TYPES + " GROUP BY boolcolumn";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BOOLCOLUMN`, " //
                + "MIN(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` " //
                + "GROUP BY `TABLE_HIVE_ALL_DATA_TYPES`.`BOOLCOLUMN`";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));

    }

    @Test
    void testAggregateHaving() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a BOOLEAN", "b DECIMAL(19,0)"), //
                List.of("true, 56"));
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "."
                + TABLE_HIVE_ALL_DATA_TYPES + " GROUP BY boolcolumn HAVING MIN(biginteger)<57";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BOOLCOLUMN`, " //
                + "MIN(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` " //
                + "GROUP BY `TABLE_HIVE_ALL_DATA_TYPES`.`BOOLCOLUMN` " //
                + "HAVING MIN(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) < 57";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    // =, !=, <, <=, >, >=
    void testComparisonPredicates() throws SQLException {
        final ResultSet expected = getExpectedResultSet(
                List.of("a DECIMAL(19,0)", "b1 BOOLEAN", "b2 BOOLEAN", "b3 BOOLEAN", "b4 BOOLEAN", "b5 BOOLEAN",
                        "b6 BOOLEAN"), //
                List.of("56, false, true, true, true, false, false"));
        final String query = "SELECT biginteger, biginteger=60, biginteger!=60, biginteger<60, biginteger<=60, biginteger>60, biginteger>=60 FROM "
                + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES + " WHERE intcol = 85";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` = 60, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` <> 60, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` < 60, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` <= 60, " //
                + "60 < `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`, "
                + "60 <= `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` WHERE `TABLE_HIVE_ALL_DATA_TYPES`.`INTCOL` = 85";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));

    }

    @Test
    // NOT, AND, OR
    void testLogicalPredicates() {
        final String query = "SELECT biginteger FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES
                + " WHERE (biginteger < 56 or biginteger > 56) AND NOT (biginteger is null)";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` "
                + "WHERE ((`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` < 56 OR 56 < `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) "
                //
                + "AND NOT ((`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) IS NULL))";
        assertAll(() -> assertThat(statementExasol.executeQuery(query).next(), equalTo(false)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    // LIKE, LIKE ESCAPE (not pushed down)
    void testLikePredicates() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a VARCHAR(10) ASCII", "b BOOLEAN"), //
                List.of("'tytu', false"));
        final String query = "SELECT varcharcol, varcharcol LIKE 't%' ESCAPE 't' FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "."
                + TABLE_HIVE_ALL_DATA_TYPES + " WHERE (varcharcol LIKE 't%')";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`VARCHARCOL` " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` WHERE `TABLE_HIVE_ALL_DATA_TYPES`.`VARCHARCOL` LIKE ''t%''";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    // REGEXP_LIKE rewritten to REGEXP
    void testLikePredicatesRewritten() {
        final String query = "SELECT varcharcol FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES
                + " WHERE varcharcol REGEXP_LIKE 'a+'";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`VARCHARCOL` " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` WHERE `TABLE_HIVE_ALL_DATA_TYPES`.`VARCHARCOL`REGEXP''a+''";
        assertAll(() -> assertThat(statementExasol.executeQuery(query).next(), equalTo(false)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    // BETWEEN, IN, IS NULL, !=NULL(rewritten to "IS NOT NULL")
    void testMiscPredicates() throws SQLException {
        final ResultSet expected = getExpectedResultSet(
                List.of("a DECIMAL(19,0)", "b1 BOOLEAN", "b2 BOOLEAN", "b3 BOOLEAN"), //
                List.of("56, true, false, true"));
        final String query = "SELECT biginteger,  biginteger in (56, 61), biginteger is null, biginteger != null FROM "
                + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES + " WHERE biginteger between 51 and 60";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` IN (56, 61), " //
                + "(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) IS NULL, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` IS NOT NULL " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` " //
                + "WHERE `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` BETWEEN 51 AND 60";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    // This does not work with the current Hive version, since datatypes for the SUM
    // columns dffer in the prepare and execute phases
    void testCountSumAggregateFunction() throws SQLException {
        final ResultSet expected = getExpectedResultSet(
                List.of("a DECIMAL(19,0)", "b DECIMAL(19,0)", "c DECIMAL(19,0)", "d DECIMAL(19,0)", "e DECIMAL(19,0)"), //
                List.of("1, 1, 1, 56, 56"));
        final String query = "SELECT COUNT(biginteger), COUNT(*), COUNT(DISTINCT biginteger), SUM(biginteger), SUM(DISTINCT biginteger) FROM "
                + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String expectedExplainVirtual = "SELECT COUNT(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`), " //
                + "COUNT(*), COUNT(DISTINCT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`), " //
                + "SUM(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`), " //
                + "SUM(DISTINCT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES`";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    void testAvgMinMaxAggregateFunction() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a DOUBLE", "b DECIMAL(19,0)", "c DECIMAL(19,0)"), //
                List.of("56.0, 56, 56"));
        final String query = "SELECT AVG(biginteger), MIN(biginteger), MAX(biginteger) FROM " + VIRTUAL_SCHEMA_HIVE_JDBC
                + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String expectedExplainVirtual = "SELECT AVG(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`), " //
                + "MIN(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`), " //
                + "MAX(`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES`";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    void testCastedStringFunctions() {
        final String qualifiedTableName = VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String query = "SELECT concat(upper(varcharcol),lower(repeat(varcharcol,2))) FROM " + qualifiedTableName;
        final String expectedExplainVirtual = "SELECT CAST(CONCAT(CAST(UPPER(`TABLE_HIVE_ALL_DATA_TYPES`.`VARCHARCOL`) " //
                + "as string),CAST(LOWER(CAST(REPEAT(`TABLE_HIVE_ALL_DATA_TYPES`.`VARCHARCOL`,2) "
                + "as string)) as string)) as string) FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES`";
        assertAll(() -> assertExpressionExecutionStringResult(query, "TYTUtytutytu"),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    void testRewrittenDivAndModFunctions() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a DECIMAL(19,0)", "b DECIMAL(19,0)"), //
                List.of("1, 0"));
        final String query = "SELECT DIV(biginteger,biginteger), mod(biginteger,biginteger) FROM "
                + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` DIV `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` % `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES`";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    void testRewrittenSubStringFunction() {
        final String qualifiedTableName = VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_HIVE_ALL_DATA_TYPES;
        final String query = "SELECT substring(stringcol FROM 1 FOR 2) FROM " + qualifiedTableName;
        final String expectedExplainVirtual = "SELECT SUBSTR(`TABLE_HIVE_ALL_DATA_TYPES`.`STRINGCOL`, 1, 2) " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES";
        assertAll(() -> assertExpressionExecutionStringResult(query, "ts"),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    public void testOrderByLimit() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a BOOLEAN", "b DECIMAL(19,0)"), //
                List.of("true, 56"));
        final String query = "SELECT  boolcolumn, biginteger FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "."
                + TABLE_HIVE_ALL_DATA_TYPES + " ORDER BY biginteger LIMIT 3";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BOOLCOLUMN` " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` " //
                + "ORDER BY `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` NULLS LAST LIMIT 3";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    public void testOrderByLimitOffset() throws SQLException {
        final ResultSet expected = getExpectedResultSet(List.of("a BOOLEAN", "b DECIMAL(19,0)"), //
                List.of("true, 56"));
        final String query = "SELECT  boolcolumn, biginteger FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "."
                + TABLE_HIVE_ALL_DATA_TYPES + " ORDER BY biginteger LIMIT 2 OFFSET 1";
        final String expectedExplainVirtual = "SELECT `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER`, " //
                + "`TABLE_HIVE_ALL_DATA_TYPES`.`BOOLCOLUMN` " //
                + "FROM `default`.`TABLE_HIVE_ALL_DATA_TYPES` " //
                + "ORDER BY `TABLE_HIVE_ALL_DATA_TYPES`.`BIGINTEGER` NULLS LAST";
        assertAll(() -> assertThat(statementExasol.executeQuery(query), matchesResultSet(expected)),
                () -> assertExplainVirtual(query, expectedExplainVirtual));
    }

    @Test
    void testNumberBeyondExasolPrecisionMaxValueToDecimalColumnTypes() throws SQLException {
        final String expectedSchemaQualifiedTableName = SCHEMA_EXASOL + ".EXPECTED";
        statementExasol.execute("CREATE OR REPLACE TABLE " + expectedSchemaQualifiedTableName //
                + "(COLUMN_NAME VARCHAR(128), " //
                + "COLUMN_TYPE VARCHAR(40))");
        statementExasol.execute("INSERT INTO " + expectedSchemaQualifiedTableName + " VALUES " //
                + "('DECIMAL_COL1', 'DECIMAL(12,6)'), " //
                + "('DECIMAL_COL2', 'DECIMAL(36,16)'), " //
                + "('DECIMAL_COL3', 'DECIMAL(36,2)') " //
        );
        final ResultSet expected = statementExasol.executeQuery("SELECT * FROM " + expectedSchemaQualifiedTableName);
        final ResultSet actual = statementExasol
                .executeQuery("SELECT COLUMN_NAME, COLUMN_TYPE FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '" //
                        + VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL + "' AND COLUMN_TABLE='" //
                        + TABLE_HIVE_DECIMAL_CAST + "' ORDER BY COLUMN_ORDINAL_POSITION");
        assertThat(actual, matchesResultSet(expected));
    }

    @Test
    void testNumberBeyondExasolPrecisionMaxValueToDecimal() throws SQLException {
        final ResultSet expected = getExpectedResultSet(
                List.of("a DECIMAL(12,6)", "b DECIMAL(36,16)", "c DECIMAL(36,2)"), //
                List.of("123456.123457, 123456789.0111111111111110, 1234444444444444444.55"));
        final String query = "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL + "."
                + TABLE_HIVE_DECIMAL_CAST;
        assertThat(statementExasol.executeQuery(query), matchesResultSet(expected));
    }
}