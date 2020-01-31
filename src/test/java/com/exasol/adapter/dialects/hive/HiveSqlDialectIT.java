package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
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

@Tag("integration")
@Testcontainers
class HiveSqlDialectIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSqlDialectIT.class);
    private static final String PATH_TO_HIVE_PROPERTY_FILE = "src/test/resources/integration/driver/hive/hive.properties";
    private static final String HIVE_SERVICE_NAME = "hive-server_1";
    private static final int HIVE_EXPOSED_PORT = 10000;
    private static final String SCHEMA_HIVE = "default";
    private static final String TABLE_HIVE_SIMPLE = "TABLE_HIVE_SIMPLE";

    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER));
    @Container
    public static DockerComposeContainer hiveContainer = new DockerComposeContainer(new File(HIVE_DOCKER_COMPOSE_YAML)) //
            .withExposedService(HIVE_SERVICE_NAME, HIVE_EXPOSED_PORT);
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
            integrationTestSetupManager.createTestTablesForJoinTests(statementHive, SCHEMA_HIVE, TABLE_JOIN_1,
                    TABLE_JOIN_2);
        }
        integrationTestSetupManager.createTestSchema(statementExasol, SCHEMA_EXASOL);
    }

    private static void createTableHiveSimple(final Statement statementHive) throws SQLException {
        statementHive.execute("CREATE TABLE " + TABLE_HIVE_SIMPLE + "(X INT)");
        statementHive.execute("TRUNCATE TABLE " + TABLE_HIVE_SIMPLE);
        statementHive.execute("INSERT INTO " + TABLE_HIVE_SIMPLE + " VALUES (99)");

//        statementHive.execute("create table t1(x int, y varchar(100))");
//        statementHive.execute("truncate table t1");
//        statementHive.execute("insert into t1 values (1,'aaa'), (2,'bbb')");
//        statementHive.execute("create table t2(x int, y varchar(100))");
//        statementHive.execute("truncate table t2");
//        statementHive.execute("insert into t2 values (2,'bbb'), (3,'ccc')");
//
//        statementHive.execute(
//                "create table decimal_cast(decimal_col1 decimal(12, 6), decimal_col2 decimal(36, 16), decimal_col3 decimal(38, 17))");
//        statementHive.execute("truncate table decimal_cast");
//        statementHive.execute(
//                "insert into decimal_cast values (123456.12345671, 123456789.011111111111111, 1234444444444444444.5555555555555555555555550)");
//
//        statementHive.execute(
//                "CREATE TABLE ALL_HIVE_DATA_TYPES(ARRAYCOL ARRAY<string>, BIGINTEGER BIGINT, BOOLCOLUMN BOOLEAN, CHARCOLUMN CHAR(1), DECIMALCOL DECIMAL(10,0), DOUBLECOL DOUBLE, FLOATCOL FLOAT, INTCOL INT, MAPCOL MAP<string,int>, SMALLINTEGER SMALLINT, STRINGCOL STRING, STRUCTCOL struct<a : int, b : int>, TIMESTAMPCOL TIMESTAMP, TINYINTEGER TINYINT, VARCHARCOL VARCHAR(10), BINARYCOL BINARY, DATECOL DATE)");
//        statementHive.execute("truncate table ALL_HIVE_DATA_TYPES");
//        statementHive.execute(
//                "insert into all_hive_data_types(arraycol,biginteger,boolcolumn,charcolumn,decimalcol,doublecol,floatcol,intcol,mapcol,smallinteger,stringcol,structcol,timestampcol,tinyinteger,varcharcol,binarycol,datecol) select array('etet','ettee'), 56, true, '2', 53, 56.3, 5.199999809265137, 85, map('jkljj',5), 2, 'tshg', named_struct('a',2,'b',4), timestamp '2017-01-02 13:32:50.744', 1, 'tytu', 'MTAxMA==', date '1970-01-01' from t");
    }

    private static Connection getHiveConnection() throws ClassNotFoundException, SQLException, InterruptedException {
//        Thread.sleep(1000000000);
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        return DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "hive");
    }

    // TODO: extract from here and Oracle
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

    @Test
    void test() throws InterruptedException {
        Thread.sleep(1000000000);
    }
}
