package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;
import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;

import utils.IntegrationTestSetupManager;

/*
 * How to run `HiveSqlDialectIT`:
 * Download Hive JDBC driver `HiveJDBC41.jar`.
 * Temporarily put it into `src/test/resources/integration/driver/hive` directory.
 * If the file's name is different from the one mentioned above, edit `src/test/resources/integration/driver/hive/hive.properties` and settings.cfg files.
 * Run the tests from an IDE or temporarily add `HiveSqlDialectIT.java` into the `maven-failsafe-plugin`'s includes section and execute `mvn verify` command.
 */
@Tag("integration")
@Testcontainers
class HiveSqlDialectIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSqlDialectIT.class);
    public static final String HIVE_DOCKER_COMPOSE_YAML = "src/test/resources/integration/driver/hive/docker-compose.yaml";
    public static final Path HIVE_DRIVER_SETTINGS_PATH = Path.of("src", "test", "resources", "integration", "driver",
            "hive", JDBC_DRIVER_CONFIGURATION_FILE_NAME);
    private static final String PATH_TO_HIVE_PROPERTY_FILE = "src/test/resources/integration/driver/hive/hive.properties";
    private static final String HIVE_SERVICE_NAME = "hive-server_1";
    private static final int HIVE_EXPOSED_PORT = 10000;
    private static final String CONNECTION_HIVE_JDBC = "CONNECTION_HIVE_JDBC";
    private static final String SCHEMA_HIVE = "default";
    private static final String HIVE_USERNAME = "hive";
    private static final String HIVE_PASSWORD = "hive";
    private static final String TABLE_HIVE_SIMPLE = "TABLE_HIVE_SIMPLE";
    private static final String TABLE_HIVE_DECIMAL_CAST = "TABLE_HIVE_DECIMAL_CAST";
    private static final String TABLE_HIVE_ALL_DATA_TYPES = "TABLE_HIVE_ALL_DATA_TYPES";
    private static final String VIRTUAL_SCHEMA_HIVE_JDBC = "VIRTUAL_SCHEMA_HIVE_JDBC";
    private static final String VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL = "VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL";
    @Container
    public static DockerComposeContainer hiveContainer = new DockerComposeContainer(new File(HIVE_DOCKER_COMPOSE_YAML)) //
            .withExposedService(HIVE_SERVICE_NAME, HIVE_EXPOSED_PORT);
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER)); //
//                    .withClusterLogsPath(Path.of("target/mylogs"));
    private static Statement statementExasol;
    private static final IntegrationTestSetupManager integrationTestSetupManager = new IntegrationTestSetupManager();

    @BeforeAll
    static void beforeAll()
            throws InterruptedException, BucketAccessException, TimeoutException, SQLException, ClassNotFoundException {
        final String driverName = getPropertyFromFile(PATH_TO_HIVE_PROPERTY_FILE, "driver.name");
        final String driverPath = getPropertyFromFile(PATH_TO_HIVE_PROPERTY_FILE, "driver.path");
        uploadFilesToBucket(driverName, driverPath);
        final Connection exasolConnection = exasolContainer.createConnectionForUser(exasolContainer.getUsername(),
                exasolContainer.getPassword());
        statementExasol = exasolConnection.createStatement();
        try (final Connection connection = getHiveConnection()) {
            final Statement statementHive = connection.createStatement();
            createTableHiveSimple(statementHive);
            createTableDecimalCast(statementHive);
            createTableAllDataTypes(statementHive);
            integrationTestSetupManager.createTestTablesForJoinTests(statementHive, SCHEMA_HIVE, TABLE_JOIN_1,
                    TABLE_JOIN_2);
        }
        integrationTestSetupManager.createTestSchema(statementExasol, SCHEMA_EXASOL);
        createJdbcConnection();
        integrationTestSetupManager.createAdapterScript(statementExasol, SCHEMA_EXASOL + "." + ADAPTER_SCRIPT_EXASOL,
                VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION,
                Optional.of("%jar /buckets/bfsdefault/default/drivers/jdbc/" + driverName + ";\n"));
        createVirtualSchema(VIRTUAL_SCHEMA_HIVE_JDBC, Optional.empty());
        createVirtualSchema(VIRTUAL_SCHEMA_HIVE_JDBC_NUMBER_TO_DECIMAL,
                Optional.of("hive_cast_number_to_decimal_with_precision_and_scale='36,2'"));
    }

    private static Connection getHiveConnection() throws ClassNotFoundException, SQLException, InterruptedException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        return DriverManager.getConnection("jdbc:hive2://localhost:" + HIVE_EXPOSED_PORT + "/" + SCHEMA_HIVE,
                HIVE_USERNAME, HIVE_PASSWORD);
    }

    private static String getPropertyFromFile(final String filePath, final String propertyName) {
        try (final InputStream inputStream = new FileInputStream(filePath)) {
            final Properties properties = new Properties();
            properties.load(inputStream);
            return properties.getProperty(propertyName);
        } catch (final IOException exception) {
            throw new IllegalArgumentException("Cannot access the oracle.properties file or read from it.");
        }
    }

    private static void uploadFilesToBucket(final String driverName, final String driverPath)
            throws InterruptedException, BucketAccessException, TimeoutException {
        final Bucket bucket = exasolContainer.getDefaultBucket();
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        bucket.uploadFile(Path.of(driverPath, driverName), "drivers/jdbc/" + driverName);
        bucket.uploadFile(HIVE_DRIVER_SETTINGS_PATH, "drivers/jdbc/" + JDBC_DRIVER_CONFIGURATION_FILE_NAME);
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

    private static void createJdbcConnection() throws SQLException {
        integrationTestSetupManager.createConnection(statementExasol, CONNECTION_HIVE_JDBC,
                "jdbc:hive2://" + DOCKER_IP_ADDRESS + ":" + HIVE_EXPOSED_PORT + "/" + SCHEMA_HIVE, //
                HIVE_USERNAME, HIVE_PASSWORD);
    }

    private static void createVirtualSchema(final String virtualSchemaName, final Optional<String> additionalParameters)
            throws SQLException {
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE VIRTUAL SCHEMA ");
        builder.append(virtualSchemaName);
        builder.append(" USING " + SCHEMA_EXASOL + "." + ADAPTER_SCRIPT_EXASOL + " WITH ");
        builder.append("SQL_DIALECT     = 'HIVE' ");
        builder.append("CONNECTION_NAME = '" + CONNECTION_HIVE_JDBC + "' ");
        builder.append("SCHEMA_NAME     = '" + SCHEMA_HIVE + "' ");
        additionalParameters.ifPresent(builder::append);
        final String sql = builder.toString();
        LOGGER.info("Creating virtual schema with query: " + sql);
        statementExasol.execute(sql);
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

    @Nested
    @DisplayName("Join test")
    class JoinTest {
        @Test
        void testInnerJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", //
                    "VALUES(2,'bbb', 2,'bbb')");
            final ResultSet actual = statementExasol
                    .executeQuery("SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a INNER JOIN  "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x=b.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testInnerJoinWithProjection() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(y VARCHAR(100))", //
                    " VALUES('bbbbbb')");
            final ResultSet actual = statementExasol
                    .executeQuery("SELECT b.y || " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + ".y FROM "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " INNER JOIN  " + VIRTUAL_SCHEMA_HIVE_JDBC
                            + "." + TABLE_JOIN_2 + " b ON " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + ".x=b.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testLeftJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", //
                    "VALUES(1, 'aaa', null, null), " //
                            + "(2, 'bbb', 2, 'bbb')");
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a LEFT OUTER JOIN  "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testRightJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", //
                    "VALUES(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a RIGHT OUTER JOIN  "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", //
                    "VALUES(1, 'aaa', null, null), " //
                            + "(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a FULL OUTER JOIN  "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testRightJoinWithComplexCondition() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", //
                    "VALUES(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a RIGHT OUTER JOIN  "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x||a.y=b.x||b.y ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoinWithComplexCondition() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", //
                    "VALUES(1, 'aaa', null, null), " //
                            + "(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT * FROM " + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_1 + " a FULL OUTER JOIN  "
                            + VIRTUAL_SCHEMA_HIVE_JDBC + "." + TABLE_JOIN_2 + " b ON a.x-b.x=0 ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }
    }

    @Test
    void testDataTypeMapping() throws SQLException {
        final String expectedSchemaQualifiedTableName = SCHEMA_EXASOL + ".EXA_DBA_COLUMNS_EXPECTED";
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
}
