package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.dialects.IntegrationTestsConstants.*;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.OracleContainer;
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
class OracleSqlDialectIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(OracleSqlDialectIT.class);
    private static final String SCHEMA_ORACLE = "SCHEMA_ORACLE";
    private static final int ORACLE_PORT = 1521;
    private static final String CONNECTION_ORACLE_JDBC = "CONNECTION_ORACLE_JDBC";
    private static final String TABLE_ORACLE_ALL_DATA_TYPES = "TABLE_ORACLE_ALL_DATA_TYPES";

    private static final String VIRTUAL_SCHEMA_ORACLE = "VIRTUAL_SCHEMA_ORACLE";
    private static final String VIRTUAL_SCHEMA_ORACLE_NUMBER_TO_DECIMAL = "VIRTUAL_SCHEMA_ORACLE_NUMBER_TO_DECIMAL";

    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER));
    @Container
    private static final OracleContainer oracleContainer = new OracleContainer(ORACLE_DOCKER_VERSION);
    private static final String PATH_TO_ORACLE_PROPERTY_FILE = "src/test/resources/integration/driver/oracle/oracle.properties";
    private static Statement statementExasol;
    private static final IntegrationTestSetupManager integrationTestSetupManager = new IntegrationTestSetupManager();

    @BeforeAll
    static void beforeAll() throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final Bucket bucket = exasolContainer.getDefaultBucket();
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        final String instantClientName = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "instant.client.name");
        final String instantClientPath = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "instant.client.path");
//        bucket.uploadFile(Path.of(instantClientPath, instantClientName), "drivers/jdbc/" + instantClientName);
        bucket.uploadFile(ORACLE_DRIVER_SETTINGS_PATH, "drivers/jdbc/" + JDBC_DRIVER_CONFIGURATION_FILE_NAME);
        final String driverName = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "driver.name");
        final String driverPath = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "driver.path");
        bucket.uploadFile(Path.of(driverPath, driverName), driverName);
        final Connection exasolConnection = exasolContainer.createConnectionForUser(exasolContainer.getUsername(),
                exasolContainer.getPassword());
        statementExasol = exasolConnection.createStatement();
        final Statement statementOracle = integrationTestSetupManager.getStatement(oracleContainer);
        integrationTestSetupManager.createTestSchema(statementExasol, SCHEMA_EXASOL);
        createOracleUser(statementOracle);
        createOracleTestTableAllDataTypes(statementOracle);
        final String connectionString = "jdbc:oracle:thin:@//" + DOCKER_IP_ADDRESS + ":"
                + oracleContainer.getMappedPort(ORACLE_PORT) + "/xe";
        System.out.println(connectionString);
        integrationTestSetupManager.createConnection(statementExasol, CONNECTION_ORACLE_JDBC, connectionString,
                oracleContainer.getUsername(), oracleContainer.getPassword());
        integrationTestSetupManager.createAdapterScript(statementExasol, SCHEMA_EXASOL + "." + ADAPTER_SCRIPT_EXASOL,
                VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION,
                Optional.of("%jar /buckets/bfsdefault/default/" + driverName + ";\n"));
        createVirtualSchema(VIRTUAL_SCHEMA_ORACLE, SCHEMA_ORACLE, Optional.empty());
        createVirtualSchema(VIRTUAL_SCHEMA_ORACLE_NUMBER_TO_DECIMAL, SCHEMA_ORACLE,
                Optional.of("oracle_cast_number_to_decimal_with_precision_and_scale='36,1'"));
    }

    private static String getPropertyFromFile(final String filePath, final String propertyName) {
        try (final InputStream inputStream = new FileInputStream(filePath)) {
            final Properties properties = new Properties();
            properties.load(inputStream);
            return properties.getProperty(propertyName);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Cannot access the oracle.properties file or read from it.");
        }
    }

    private static void createOracleUser(final Statement statementOracle) throws SQLException {
        final String username = SCHEMA_ORACLE;
        final String password = SCHEMA_ORACLE;
        statementOracle.execute("CREATE USER " + username + " IDENTIFIED BY " + password);
        statementOracle.execute("GRANT CONNECT TO " + username);
        statementOracle.execute("GRANT CREATE SESSION TO " + username);
        statementOracle.execute("GRANT UNLIMITED TABLESPACE TO " + username);
    }

    private static void createVirtualSchema(final String virtualSchemaName, final String originSchemaName,
            final Optional<String> additionalParameters) throws SQLException {
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE VIRTUAL SCHEMA ");
        builder.append(virtualSchemaName);
        builder.append(" USING " + SCHEMA_EXASOL + "." + ADAPTER_SCRIPT_EXASOL + " WITH ");
        builder.append("SQL_DIALECT     = 'ORACLE' ");
        builder.append("CONNECTION_NAME = '" + CONNECTION_ORACLE_JDBC + "' ");
        builder.append("SCHEMA_NAME     = '" + originSchemaName + "' ");
        additionalParameters.ifPresent(builder::append);
        final String sql = builder.toString();
        LOGGER.info("Creating virtual schema with query: " + sql);
        statementExasol.execute(sql);
    }

    private static void createOracleTestTableAllDataTypes(final Statement statementOracle) throws SQLException {
        final String qualifiedTableName = SCHEMA_ORACLE + "." + TABLE_ORACLE_ALL_DATA_TYPES;
        statementOracle.execute("CREATE TABLE " + qualifiedTableName + " (" //
                + "c1 char(50),	" //
                + "c2 nchar(50), " //
                + "c3 varchar2(50), " //
                + "c4 nvarchar2(50), " //
                + "c5 number, " //
                + "c_number36 number(36), " //
                + "c6 number(38), " //
                + "c7 number(10,5), " //
                + "c_binfloat binary_float, " //
                + "c_bindouble binary_double, " //
                + "c10 date, " //
                + "c11 timestamp(3), " //
                + "c12 timestamp, " //
                + "c13 timestamp(9), " //
                + "c14 timestamp with time zone, " //
                + "c15 timestamp with local time zone, " //
                + "c16 interval year to month, " //
                + "c17 interval day to second, " //
                + "c18 blob, " //
                + "c19 clob,	" //
                + "c20 nclob, " //
                + "c_float float, " //
                + "c_float126 float(126), " //
                + "c_long long " //
                + ")");
        statementOracle.execute("INSERT INTO " + qualifiedTableName + " VALUES (" //
                + "'aaaaaaaaaaaaaaaaaaaa', " //
                + "'bbbbbbbbbbbbbbbbbbbb', " //
                + "'cccccccccccccccccccc', " //
                + "'dddddddddddddddddddd', " //
                + "123456789012345678901234567890123456, " // C5
                + "123456789012345678901234567890123456, " // c_number36
                + "12345678901234567890123456789012345678, " // C6
                + "12345.12345, " // C7
                + "1234.1241723, " // C_BINFLOAT
                + "1234987.120871234, " // C_BINDOUBLE
                + "TO_DATE('2016-08-19', 'YYYY-MM-DD'), " // C10
                + "TO_TIMESTAMP('2013-03-11 17:30:15.123', 'YYYY-MM-DD HH24:MI:SS.FF'), " // C11
                + "TO_TIMESTAMP('2013-03-11 17:30:15.123456', 'YYYY-MM-DD HH24:MI:SS.FF'), " // C12
                + "TO_TIMESTAMP('2013-03-11 17:30:15.123456789', 'YYYY-MM-DD HH24:MI:SS.FF'), " // C13
                + "TO_TIMESTAMP_TZ('2016-08-19 11:28:05 -08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), " // C14
                + "TO_TIMESTAMP_TZ('2018-04-30 10:00:05 -08:00', 'YYYY-MM-DD HH24:MI:SS TZH:TZM'), " // C15
                + "'54-2', " // C16
                + "'1 11:12:10.123', " // C17
                + "'0102030405060708090a0b0c0d0e0f', " // C18
                + "'0987asdlfkjq2222qawsf;lkja09ed8q2w;43lkrjasdf09uqaw43lkjra0-98sf[iqjw4,mfas[dpiuj[qa09w44', " // C19
                + "'0987asdlfkjq2222qawsf;lkja09ed8q2w;43lkrjasdf09uqaw43lkjra0-98sf[iqjw4,mfas[dpiuj[qa09w44', " // C20
                + "12345.01982348239, " // c_float
                + "12345678.01234567901234567890123456789, " // c_float126
                + "'test long 123' " // long
                + ")");
    }

    @Test
    void test() {
        oracleContainer.isRunning();
    }
}