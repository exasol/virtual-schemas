package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;
import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    private static final String CONNECTION_ORACLE_ORA = "CONNECTION_ORACLE_ORA";
    private static final String TABLE_ORACLE_ALL_DATA_TYPES = "TABLE_ORACLE_ALL_DATA_TYPES";
    private static final String TABLE_ORACLE_NUMBER_HANDLING = "TABLE_ORACLE_NUMBER_HANDLING";

    private static final String VIRTUAL_SCHEMA_JDBC = "VIRTUAL_SCHEMA_JDBC";
    private static final String VIRTUAL_SCHEMA_ORA = "VIRTUAL_SCHEMA_ORA";
    private static final String VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL = "VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL";
    private static final String VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL = "VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL";

    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER)).withClusterLogsPath(Path.of("target/mylogs"));
    @Container
    private static final OracleContainer oracleContainer = new OracleContainer(ORACLE_DOCKER_VERSION);
    private static final String PATH_TO_ORACLE_PROPERTY_FILE = "src/test/resources/integration/driver/oracle/oracle.properties";
    private static Statement statementExasol;
    private static final IntegrationTestSetupManager integrationTestSetupManager = new IntegrationTestSetupManager();

    @BeforeAll
    static void beforeAll() throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final String driverName = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "driver.name");
        final String driverPath = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "driver.path");
        final String instantClientName = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "instant.client.name");
        final String instantClientPath = getPropertyFromFile(PATH_TO_ORACLE_PROPERTY_FILE, "instant.client.path");
        uploadFilesToBucket(driverName, driverPath, instantClientName, instantClientPath);
        final Connection exasolConnection = exasolContainer.createConnectionForUser(exasolContainer.getUsername(),
                exasolContainer.getPassword());
        statementExasol = exasolConnection.createStatement();
        final Statement statementOracle = integrationTestSetupManager.getStatement(oracleContainer);
        integrationTestSetupManager.createTestSchema(statementExasol, SCHEMA_EXASOL);
        createOracleUser(statementOracle);
        createOracleTestTableAllDataTypes(statementOracle);
        createOracleTestTableNumberHandling(statementOracle);
        integrationTestSetupManager.createTestTablesForJoinTests(statementOracle, SCHEMA_ORACLE, TABLE_JOIN_1,
                TABLE_JOIN_2);
        final Integer mappedPort = oracleContainer.getMappedPort(ORACLE_PORT);
        final String oracleContainerUsername = oracleContainer.getUsername();
        final String oracleContainerPassword = oracleContainer.getPassword();
        createJdbcConnection(mappedPort, oracleContainerUsername, oracleContainerPassword);
        createOraConnection(mappedPort, oracleContainerUsername, oracleContainerPassword);
        integrationTestSetupManager.createAdapterScript(statementExasol, SCHEMA_EXASOL + "." + ADAPTER_SCRIPT_EXASOL,
                VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION,
                Optional.of("%jar /buckets/bfsdefault/default/drivers/jdbc/" + driverName + ";\n"));
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC, SCHEMA_ORACLE, Optional.empty());
        createVirtualSchema(VIRTUAL_SCHEMA_ORA, SCHEMA_ORACLE,
                Optional.of("IMPORT_FROM_ORA='true' ORA_CONNECTION_NAME='" + CONNECTION_ORACLE_ORA + "'"));
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, SCHEMA_ORACLE,
                Optional.of("oracle_cast_number_to_decimal_with_precision_and_scale='36,1'"));
        createVirtualSchema(VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL, SCHEMA_ORACLE, Optional.of(
                " oracle_cast_number_to_decimal_with_precision_and_scale='36,1' IMPORT_FROM_ORA='true' ORA_CONNECTION_NAME='"
                        + CONNECTION_ORACLE_ORA + "'"));
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

    private static void uploadFilesToBucket(final String driverName, final String driverPath,
            final String instantClientName, final String instantClientPath)
            throws InterruptedException, BucketAccessException, TimeoutException {
        final Bucket bucket = exasolContainer.getDefaultBucket();
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        bucket.uploadFile(Path.of(driverPath, driverName), "drivers/jdbc/" + driverName);
        bucket.uploadFile(Path.of(instantClientPath, instantClientName), "drivers/oracle/" + instantClientName);
        bucket.uploadFile(ORACLE_DRIVER_SETTINGS_PATH, "drivers/jdbc/" + JDBC_DRIVER_CONFIGURATION_FILE_NAME);
    }

    private static void createOracleUser(final Statement statementOracle) throws SQLException {
        final String username = SCHEMA_ORACLE;
        final String password = SCHEMA_ORACLE;
        statementOracle.execute("CREATE USER " + username + " IDENTIFIED BY " + password);
        statementOracle.execute("GRANT CONNECT TO " + username);
        statementOracle.execute("GRANT CREATE SESSION TO " + username);
        statementOracle.execute("GRANT UNLIMITED TABLESPACE TO " + username);
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

    private static void createOracleTestTableNumberHandling(final Statement statementOracle) throws SQLException {
        final String qualifiedTableName = SCHEMA_ORACLE + "." + TABLE_ORACLE_NUMBER_HANDLING;
        statementOracle.execute("CREATE TABLE " + qualifiedTableName + " (" //
                + "a number,	" //
                + "b number(38, 10), " //
                + "c number(36,2) " //
                + ")");
        statementOracle.execute("INSERT INTO " + qualifiedTableName + " VALUES (" //
                + "1234567890123456789012345678901234.56, " //
                + "1234567890123456789012345678.9012345678, " //
                + "1234567890123456789012345678901234.56 " //
                + ")");
    }

    private static void createJdbcConnection(final Integer mappedPort, final String oracleContainerUsername,
            final String oracleContainerPassword) throws SQLException {
        integrationTestSetupManager.createConnection(statementExasol, CONNECTION_ORACLE_JDBC,
                "jdbc:oracle:thin:@//" + DOCKER_IP_ADDRESS + ":" + mappedPort + "/xe", //
                oracleContainerUsername, oracleContainerPassword);
    }

    private static void createOraConnection(final Integer mappedPort, final String oracleContainerUsername,
            final String oracleContainerPassword) throws SQLException {
        integrationTestSetupManager.createConnection(statementExasol, CONNECTION_ORACLE_ORA, //
                "(DESCRIPTION =" //
                        + "(ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)" //
                        + "(HOST = " + DOCKER_IP_ADDRESS + " )" //
                        + "(PORT = " + mappedPort + ")))" //
                        + "(CONNECT_DATA = (SERVER = DEDICATED)" //
                        + "(SERVICE_NAME = xe)))" //
                , oracleContainerUsername, oracleContainerPassword);
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
        builder.append("DEBUG_ADDRESS = '10.0.2.15:3000'");
        builder.append("LOG_LEVEL = 'ALL'");
        additionalParameters.ifPresent(builder::append);
        final String sql = builder.toString();
        LOGGER.info("Creating virtual schema with query: " + sql);
        statementExasol.execute(sql);
    }

    @Nested
    @DisplayName("Number handling test")
    class numberHandlingTest {
        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testNumberToDecimal(final String virtualSchemaName) {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_ALL_DATA_TYPES;
            final String query = "SELECT c5 FROM " + qualifiedTableName;
            final SQLException exception = assertThrows(SQLException.class, () -> statementExasol.execute(query));
            assertThat(exception.getMessage(),
                    containsString("value larger than specified precision allowed for this column"));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testNumber36ToDecimal(final String virtualSchemaName) throws SQLException {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_ALL_DATA_TYPES;
            final String query = "SELECT c_number36 FROM " + qualifiedTableName;
            assertExpressionExecutionBigDecimalResult(query, new BigDecimal("123456789012345678901234567890123456"));
            assertThat(getColumnTypesOfTable(qualifiedTableName, "C_NUMBER36"), equalTo("DECIMAL(36,0)"));
        }

        private void assertExpressionExecutionBigDecimalResult(final String query, final BigDecimal expectedValue)
                throws SQLException {
            final ResultSet result = statementExasol.executeQuery(query);
            result.next();
            final BigDecimal actualResult = result.getBigDecimal(1);
            assertThat(actualResult.stripTrailingZeros(), equalTo(expectedValue));
        }

        private String getColumnTypesOfTable(final String tableName, final String columnName) throws SQLException {
            final ResultSet result = statementExasol.executeQuery("DESCRIBE " + tableName);
            while (result.next()) {
                if (result.getString("COLUMN_NAME").toUpperCase().equals(columnName)) {
                    return result.getString("SQL_TYPE").toUpperCase();
                }
            }
            throw new IllegalArgumentException("Type for column " + columnName + " not found");
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testNumber38ToDecimal(final String virtualSchemaName) {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_ALL_DATA_TYPES;
            final String query = "SELECT c6 FROM " + qualifiedTableName;
            final SQLException exception = assertThrows(SQLException.class, () -> statementExasol.execute(query));
            assertThat(exception.getMessage(),
                    containsString("value larger than specified precision allowed for this column"));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testNumber10S5ToDecimal(final String virtualSchemaName) {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_ALL_DATA_TYPES;
            final String query = "SELECT C7 FROM " + qualifiedTableName;
            assertAll(() -> assertExpressionExecutionBigDecimalResult(query, new BigDecimal("12345.12345")),
                    () -> assertThat(getColumnTypesOfTable(qualifiedTableName, "C7"), equalTo("DECIMAL(10,5)")));
        }

        @Test
        void testSelectAllColsNumberFromJDBC() throws SQLException {
            final String qualifiedTableNameActual = VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL + "."
                    + TABLE_ORACLE_NUMBER_HANDLING;
            final String qualifiedTableNameExpected = SCHEMA_EXASOL + "." + "EXPECTED";
            statementExasol.execute("CREATE OR REPLACE TABLE " + qualifiedTableNameExpected //
                    + "(A DECIMAL(36,1), " //
                    + "B DECIMAL(36,1), " //
                    + "C DECIMAL(36,2))");
            statementExasol.execute("INSERT INTO " + qualifiedTableNameExpected + " VALUES"//
                    + "(1234567890123456789012345678901234.6, 1234567890123456789012345678.9,"
                    + "                        1234567890123456789012345678901234.56)");
            assertThat(statementExasol.executeQuery("SELECT * FROM " + qualifiedTableNameActual), //
                    matchesResultSet(statementExasol.executeQuery("SELECT * FROM " + qualifiedTableNameExpected)));
        }

        @Test
        void testSelectAllColsNumberFromOra() throws SQLException {
            final String qualifiedTableNameActual = VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL + "."
                    + TABLE_ORACLE_NUMBER_HANDLING;
            final String qualifiedTableNameExpected = SCHEMA_EXASOL + "." + "EXPECTED";
            statementExasol.execute("CREATE OR REPLACE TABLE " + qualifiedTableNameExpected //
                    + "(A VARCHAR(100), " //
                    + "B VARCHAR(100), " //
                    + "C VARCHAR(100))");
            statementExasol.execute("INSERT INTO " + qualifiedTableNameExpected + " VALUES"//
                    + "('12.3456789012345678901234567890123460E32', '12.3456789012345678901234567890E26',"
                    + "                        '12.3456789012345678901234567890123456E32')");
            assertThat(statementExasol.executeQuery("SELECT * FROM " + qualifiedTableNameActual), //
                    matchesResultSet(statementExasol.executeQuery("SELECT * FROM " + qualifiedTableNameExpected)));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testNumberDataTypes(final String virtualSchemaName) {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_NUMBER_HANDLING;
            assertAll(() -> assertThat(getColumnTypesOfTable(qualifiedTableName, "A"), equalTo("DECIMAL(36,1)")),
                    () -> assertThat(getColumnTypesOfTable(qualifiedTableName, "B"), equalTo("DECIMAL(36,1)")),
                    () -> assertThat(getColumnTypesOfTable(qualifiedTableName, "C"), equalTo("DECIMAL(36,2)")));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testSelectOneNumberColumn(final String virtualSchemaName) {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_NUMBER_HANDLING;
            assertAll(
                    () -> assertExpressionExecutionBigDecimalResult("SELECT A FROM " + qualifiedTableName,
                            new BigDecimal("1234567890123456789012345678901234.6")),
                    () -> assertExpressionExecutionBigDecimalResult("SELECT B FROM " + qualifiedTableName,
                            new BigDecimal("1234567890123456789012345678.9")),
                    () -> assertExpressionExecutionBigDecimalResult("SELECT C FROM " + qualifiedTableName,
                            new BigDecimal("1234567890123456789012345678901234.56")));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL })
        void testSelectAllNunberColumnsExplainVirtual(final String virtualSchemaName) throws SQLException {
            final String qualifiedTableName = virtualSchemaName + "." + TABLE_ORACLE_NUMBER_HANDLING;
            assertExplainVirtual("SELECT * FROM " + qualifiedTableName,
                    "SELECT CAST(\"A\" AS DECIMAL(36,1)), CAST(\"B\" AS DECIMAL(36,1)), \"C\"");
        }

        private void assertExplainVirtual(final String query, final String expected) throws SQLException {
            final ResultSet explainVirtual = statementExasol.executeQuery("EXPLAIN VIRTUAL " + query);
            explainVirtual.next();
            final String explainVirtualStringActual = explainVirtual.getString("PUSHDOWN_SQL");
            MatcherAssert.assertThat(explainVirtualStringActual, containsString(expected));
        }
    }

    @Nested
    @DisplayName("Join test")
    class JoinTest {
        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testInnerJoinJdbc(final String virtualSchema) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x VARCHAR(100), y VARCHAR(100), a VARCHAR(100), b VARCHAR(100))",
                    "VALUES('2','bbb', '2','bbb')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + virtualSchema + "." + TABLE_JOIN_1
                    + " a INNER JOIN  " + virtualSchema + "." + TABLE_JOIN_2 + " b ON a.x=b.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testInnerJoinWithProjection(final String virtualSchemaName) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(y VARCHAR(100))", " VALUES('bbbbbb')");
            final String qualifiedJoinTableName1 = virtualSchemaName + "." + TABLE_JOIN_1;
            final String qualifiedJoinTableName2 = virtualSchemaName + "." + TABLE_JOIN_2;
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT b.y || " + qualifiedJoinTableName1 + ".y FROM " + qualifiedJoinTableName1 + " INNER JOIN  "
                            + qualifiedJoinTableName2 + " b ON " + qualifiedJoinTableName1 + ".x=b.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testLeftJoin(final String virtualSchemaName) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x  VARCHAR(100), y VARCHAR(100), a  VARCHAR(100), b VARCHAR(100))",
                    "VALUES('1', 'aaa', null, null), " //
                            + "('2', 'bbb', '2', 'bbb')");
            final String qualifiedJoinTableName1 = virtualSchemaName + "." + TABLE_JOIN_1;
            final String qualifiedJoinTableName2 = virtualSchemaName + "." + TABLE_JOIN_2;
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + qualifiedJoinTableName1
                    + " a LEFT OUTER JOIN  " + qualifiedJoinTableName2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testRightJoin(final String virtualSchemaName) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x VARCHAR(100), y VARCHAR(100), a VARCHAR(100), b VARCHAR(100))",
                    "VALUES('2', 'bbb', '2', 'bbb'), " //
                            + "(null, null, '3', 'ccc')");
            final String qualifiedJoinTableName1 = virtualSchemaName + "." + TABLE_JOIN_1;
            final String qualifiedJoinTableName2 = virtualSchemaName + "." + TABLE_JOIN_2;
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + qualifiedJoinTableName1
                    + " a RIGHT OUTER JOIN  " + qualifiedJoinTableName2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testFullOuterJoin(final String virtualSchemaName) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x VARCHAR(100), y VARCHAR(100), a VARCHAR(100), b VARCHAR(100))",
                    "VALUES(1, 'aaa', null, null), " //
                            + "('2', 'bbb', '2', 'bbb'), " //
                            + "(null, null, '3', 'ccc')");
            final String qualifiedJoinTableName1 = virtualSchemaName + "." + TABLE_JOIN_1;
            final String qualifiedJoinTableName2 = virtualSchemaName + "." + TABLE_JOIN_2;
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + qualifiedJoinTableName1
                    + " a FULL OUTER JOIN  " + qualifiedJoinTableName2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testRightJoinWithComplexCondition(final String virtualSchemaName) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x VARCHAR(100), y VARCHAR(100), a VARCHAR(100), b VARCHAR(100))", //
                    "VALUES('2', 'bbb', '2', 'bbb'), " //
                            + "(null, null, '3', 'ccc')");
            final String qualifiedJoinTableName1 = virtualSchemaName + "." + TABLE_JOIN_1;
            final String qualifiedJoinTableName2 = virtualSchemaName + "." + TABLE_JOIN_2;
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + qualifiedJoinTableName1
                    + " a RIGHT OUTER JOIN  " + qualifiedJoinTableName2 + " b ON a.x||a.y=b.x||b.y ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @ParameterizedTest
        @ValueSource(strings = { VIRTUAL_SCHEMA_JDBC, VIRTUAL_SCHEMA_ORA })
        void testFullOuterJoinWithComplexCondition(final String virtualSchemaName) throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL, "(x VARCHAR(100), y VARCHAR(100), a VARCHAR(100), b VARCHAR(100))", //
                    "VALUES('1', 'aaa', null, null), " //
                            + "('2', 'bbb', '2', 'bbb'), " //
                            + "(null, null, '3', 'ccc')");
            final String qualifiedJoinTableName1 = virtualSchemaName + "." + TABLE_JOIN_1;
            final String qualifiedJoinTableName2 = virtualSchemaName + "." + TABLE_JOIN_2;
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + qualifiedJoinTableName1
                    + " a FULL OUTER JOIN  " + qualifiedJoinTableName2 + " b ON a.x-b.x=0 ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }
    }

    @Nested
    @DisplayName("Datatype tests")
    class DatatypeTest {
    }
}