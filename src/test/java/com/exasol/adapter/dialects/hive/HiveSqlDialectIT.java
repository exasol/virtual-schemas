package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

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
 * If the file's name is different from the one mentioned above, edit `src/test/resources/integration/driver/hive/hive.properties` file.
 * Run the tests from an IDE or temporarily add `OracleSqlDialectIT.java` into the `maven-failsafe-plugin`'s includes section and execute `mvn verify` command.
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
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER)) //
                    .withClusterLogsPath(Path.of("target/mylogs"));
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
}
