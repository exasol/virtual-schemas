package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.IntegrationTestsConstants.*;
import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.*;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import com.exasol.jdbc.DataException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import utils.IntegrationTestSetupManager;

@Tag("integration")
@Testcontainers
class PostgreSQLSqlDialectIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSqlDialectIT.class);
    private static final String CONNECTION_POSTGRES_JDBC = "CONNECTION_POSTGRES_JDBC";
    private static final String SCHEMA_POSTGRES_TEST = "schema_postgres_test";
    private static final String SCHEMA_POSTGRES_UPPERCASE_TABLE = "schema_postgres_upper";
    private static final String TABLE_POSTGRES_SIMPLE = "table_postgres_simple";
    private static final String TABLE_POSTGRES_MIXED_CASE = "Table_Postgres_Mixed_Case";
    private static final String TABLE_POSTGRES_LOWER_CASE = "table_postgres_lower_case";
    private static final String TABLE_POSTGRES_ALL_DATA_TYPES = "table_postgres_all_data_types";
    private static final String VIRTUAL_SCHEMA_POSTGRES = "VIRTUAL_SCHEMA_POSTGRES";
    private static final String VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE = "VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE";
    private static final String VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE = "VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE";
    private static final String QUALIFIED_TABLE_JOIN_NAME_1 = VIRTUAL_SCHEMA_POSTGRES + "." + TABLE_JOIN_1;
    private static final String QUALIFIED_TABLE_JOIN_NAME_2 = VIRTUAL_SCHEMA_POSTGRES + "." + TABLE_JOIN_2;
    private static final String DOCKER_IP_ADDRESS = "172.17.0.1";
    private static final int POSTGRES_PORT = 5432;

    @Container
    private static final PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer(POSTGRES_DOCKER_VERSION);
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static Statement statementExasol;
    private static final IntegrationTestSetupManager integrationTestSetupManager = new IntegrationTestSetupManager();

    @BeforeAll
    static void beforeAll() throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final Bucket bucket = exasolContainer.getDefaultBucket();
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        bucket.uploadFile(PATH_TO_POSTGRES_DRIVER, POSTGRES_DRIVER_NAME_AND_VERSION);
        final Connection exasolConnection = exasolContainer.createConnectionForUser(exasolContainer.getUsername(),
                exasolContainer.getPassword());
        statementExasol = exasolConnection.createStatement();
        final Statement statementPostgres = getStatement(postgresqlContainer);
        integrationTestSetupManager.createTestSchema(statementExasol, SCHEMA_EXASOL_TEST);
        integrationTestSetupManager.createTestSchema(statementPostgres, SCHEMA_POSTGRES_TEST);
        integrationTestSetupManager.createTestSchema(statementPostgres, SCHEMA_POSTGRES_UPPERCASE_TABLE);
        createPostgresTestTableSimple(statementPostgres);
        createPostgresTestTableAllDataTypes(statementPostgres);
        createPostgresTestTableMixedCase(statementPostgres);
        createPostgresTestTableLowerCase(statementPostgres);
        integrationTestSetupManager.createTestTablesForJoinTests(statementPostgres, SCHEMA_POSTGRES_TEST, TABLE_JOIN_1,
                TABLE_JOIN_2);
        final String connectionString = "jdbc:postgresql://" + DOCKER_IP_ADDRESS + ":"
                + postgresqlContainer.getMappedPort(POSTGRES_PORT) + "/" + postgresqlContainer.getDatabaseName();
        System.out.println(connectionString);
        integrationTestSetupManager.createConnection(statementExasol, CONNECTION_POSTGRES_JDBC, connectionString,
                postgresqlContainer.getUsername(), postgresqlContainer.getPassword());
        integrationTestSetupManager.createAdapterScript(statementExasol,
                SCHEMA_EXASOL_TEST + "." + ADAPTER_SCRIPT_EXASOL, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION,
                Optional.of("%jar /buckets/bfsdefault/default/" + POSTGRES_DRIVER_NAME_AND_VERSION + ";\n"));
        createVirtualSchema(VIRTUAL_SCHEMA_POSTGRES, SCHEMA_POSTGRES_TEST, Optional.empty());
        createVirtualSchema(VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE, SCHEMA_POSTGRES_UPPERCASE_TABLE,
                Optional.of("ignore_errors='POSTGRESQL_UPPERCASE_TABLES'"));
        createVirtualSchema(VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE, SCHEMA_POSTGRES_UPPERCASE_TABLE,
                Optional.of("POSTGRESQL_IDENTIFIER_MAPPING = 'PRESERVE_ORIGINAL_CASE'"));
    }

    private static Statement getStatement(final JdbcDatabaseContainer container) throws SQLException {
        final DataSource dataSource = getDataSource(container);
        return dataSource.getConnection().createStatement();
    }

    private static DataSource getDataSource(final JdbcDatabaseContainer container) {
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }

    private static void createPostgresTestTableSimple(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES_TEST + "." + TABLE_POSTGRES_SIMPLE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT)");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (1)");
    }

    private static void createPostgresTestTableAllDataTypes(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES_TEST + "." + TABLE_POSTGRES_ALL_DATA_TYPES;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (" //
                + "myBigint BIGINT,	" //
                + "myBigserial BIGSERIAL, " //
                + "myBit BIT, " //
                + "myBitVar BIT VARYING(5), " //
                + "myBoolean BOOLEAN, " //
                + "myBox BOX, " //
                + "myBytea BYTEA, " //
                + "myCharacter CHARACTER(1000), " //
                + "myCharacterVar CHARACTER VARYING(1000), " //
                + "myCidr CIDR, " //
                + "myCircle CIRCLE, " //
                + "myDate DATE, " //
                + "myDouble DOUBLE PRECISION, " //
                + "myInet INET, " //
                + "myInteger INTEGER, " //
                + "myInterval INTERVAL, " //
                + "myJson JSON, " //
                + "myJsonB JSONB, " //
                + "myLine LINE, " //
                + "myLseg LSEG,	" //
                + "myMacAddr MACADDR, " //
                + "myMoney MONEY, " //
                + "myNumeric NUMERIC(36, 10), " //
                + "myPath PATH, " //
                + "myPoint POINT, " //
                + "myPolygon POLYGON, " //
                + "myReal REAL, " //
                + "mySmallint SMALLINT, " //
                + "myText TEXT, " //
                + "myTime TIME, " //
                + "myTimeWithTimeZone TIME WITH TIME ZONE, " //
                + "myTimestamp TIMESTAMP, " //
                + "myTimestampWithTimeZone TIMESTAMP WITH TIME ZONE, " //
                + "myTsquery TSQUERY, " //
                + "myTsvector TSVECTOR, " //
                + "myUuid UUID, " //
                + "myXml XML " //
                + ")");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (" //
                + "10000000000, " // myBigint
                + "nextval('" + qualifiedTableName + "_myBigserial_seq'::regclass), " // myBigserial
                + "B'1', " // myBit
                + "B'0', " // myBitVar
                + "false, " // myBoolean
                + "'( ( 1 , 8 ) , ( 4 , 16 ) )', " // myBox
                + "E'\\\\000'::bytea, " // myBytea
                + "'hajksdf', " // myCharacter
                + "'hjkdhjgfh', " // myCharacterVar
                + "'192.168.100.128/25'::cidr, " // myCidr
                + "'( ( 1 , 5 ) , 3 )'::circle, " // myCircle
                + "'2010-01-01', " // myDate
                + "192189234.1723854, " // myDouble
                + "'192.168.100.128'::inet, " // myInet
                + "7189234, " // myInteger
                + "INTERVAL '1' YEAR, " // myInterval
                + "'{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'::json, " // myJson
                + "'{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'::jsonb, " // myJsonB
                + "'{ 1, 2, 3 }'::line, " // myLine
                + "'[ ( 1 , 2 ) , ( 3 , 4 ) ]'::lseg, " // myLseg
                + "'08:00:2b:01:02:03'::macaddr, " // myMacAddr
                + "100.01, " // myMoney
                + "24.23, " // myNumeric
                + "'[ ( 1 , 2 ) , ( 3 , 4 ) ]'::path, " // myPath
                + "'( 1 , 3 )'::point, " // myPoint
                + "'( ( 1 , 2 ) , (2,4),(3,7) )'::polygon, " // myPolygon
                + "10.12, " // myReal
                + "100, " // mySmallint
                + "'This cat is super cute', " // myText
                + "'11:11:11', " // myTime
                + "'11:11:11 +01:00', " // myTimeWithTimeZone
                + "'2010-01-01 11:11:11', " // myTimestamp
                + "'2010-01-01 11:11:11 +01:00', " // myTimestampwithtimezone
                + "'fat & rat'::tsquery, " // myTsquery
                + "to_tsvector('english', 'The Fat Rats'), " // myTsvector
                + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid, " // myUuid
                + "XMLPARSE (DOCUMENT '<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>') " // myXml
                + ")");
    }

    private static void createPostgresTestTableMixedCase(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES_UPPERCASE_TABLE + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"";
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT, \"Y\" INT)");
    }

    private static void createPostgresTestTableLowerCase(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES_UPPERCASE_TABLE + "." + TABLE_POSTGRES_LOWER_CASE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT, y INT)");
    }

    private static void createVirtualSchema(final String virtualSchemaName, final String originSchemaName,
            final Optional<String> additionalParameters) throws SQLException {
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE VIRTUAL SCHEMA ");
        builder.append(virtualSchemaName);
        builder.append(" USING " + SCHEMA_EXASOL_TEST + "." + ADAPTER_SCRIPT_EXASOL + " WITH ");
        builder.append("SQL_DIALECT     = 'POSTGRESQL' ");
        builder.append("CATALOG_NAME     = '" + postgresqlContainer.getDatabaseName() + "' ");
        builder.append("CONNECTION_NAME = '" + CONNECTION_POSTGRES_JDBC + "' ");
        builder.append("SCHEMA_NAME     = '" + originSchemaName + "' ");
        additionalParameters.ifPresent(builder::append);
        final String sql = builder.toString();
        LOGGER.info("Creating virtual schema with query: " + sql);
        statementExasol.execute(sql);
    }

    @Test
    void testSelectSingleColumn() throws SQLException {
        final String qualifiedExpectedTableName = SCHEMA_EXASOL_TEST + ".TABLE_POSTGRES_SIMPLE_EXPECTED";
        statementExasol.execute("CREATE OR REPLACE TABLE " + qualifiedExpectedTableName + " (x INT)");
        statementExasol.execute("INSERT INTO " + qualifiedExpectedTableName + " VALUES (1)");
        final ResultSet expectedResultSet = statementExasol.executeQuery("SELECT * FROM " + qualifiedExpectedTableName);
        final ResultSet actualResultSet = statementExasol
                .executeQuery("SELECT * FROM " + VIRTUAL_SCHEMA_POSTGRES + "." + TABLE_POSTGRES_SIMPLE);
        assertThat(actualResultSet, matchesResultSet(expectedResultSet));
    }

    @Nested
    @DisplayName("Join test")
    class JoinTest {
        @Test
        void testInnerJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", "VALUES(2,'bbb', 2,'bbb')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                    + " a INNER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testInnerJoinWithProjection() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(y VARCHAR(100))", " VALUES('bbbbbb')");
            final ResultSet actual = statementExasol.executeQuery("SELECT b.y || " + QUALIFIED_TABLE_JOIN_NAME_1
                    + ".y FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " INNER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2
                    + " b ON " + QUALIFIED_TABLE_JOIN_NAME_1 + ".x=b.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testLeftJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))",
                    "VALUES(1, 'aaa', null, null), " //
                            + "(2, 'bbb', 2, 'bbb')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                    + " a LEFT OUTER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testRightJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", "VALUES(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                    + " a RIGHT OUTER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoin() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))",
                    "VALUES(1, 'aaa', null, null), " //
                            + "(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                    + " a FULL OUTER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testRightJoinWithComplexCondition() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))", "VALUES(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                    + " a RIGHT OUTER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x||a.y=b.x||b.y ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testFullOuterJoinWithComplexCondition() throws SQLException {
            final ResultSet expected = integrationTestSetupManager.getSelectAllFromJoinExpectedTable(statementExasol,
                    SCHEMA_EXASOL_TEST, "(x INT, y VARCHAR(100), a INT, b VARCHAR(100))",
                    "VALUES(1, 'aaa', null, null), " //
                            + "(2, 'bbb', 2, 'bbb'), " //
                            + "(null, null, 3, 'ccc')");
            final ResultSet actual = statementExasol.executeQuery("SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                    + " a FULL OUTER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x-b.x=0 ORDER BY a.x");
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }
    }

    @Nested
    @DisplayName("Identifier Test - CONVERT_TO_UPPER mode")
    class IdentifierTestConvertToUpper {
        @Test
        void testCreateSchemaWithUpperCaseTablesThrowsException() {
            final Exception exception = assertThrows(DataException.class, () -> //
            createVirtualSchema("WRONG_VIRTUAL_SCHEMA", SCHEMA_POSTGRES_UPPERCASE_TABLE, Optional.empty()));
            assertThat(exception.getMessage(), containsString("Table " + TABLE_POSTGRES_MIXED_CASE
                    + " cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation."));
        }

        @Test
        void testQueryUpperCaseTableQuotedThrowsException() {
            final Exception exception = assertThrows(SQLException.class, () -> statementExasol
                    .execute("SELECT x FROM  " + SCHEMA_EXASOL_TEST + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\""));
            assertThat(exception.getMessage(), containsString(
                    "object \"" + SCHEMA_EXASOL_TEST + "\".\"" + TABLE_POSTGRES_MIXED_CASE + "\" not found"));
        }

        @Test
        void testQueryUpperCaseTableThrowsException() {
            final Exception exception = assertThrows(SQLException.class, () -> statementExasol
                    .execute("SELECT x FROM  " + SCHEMA_EXASOL_TEST + "." + TABLE_POSTGRES_MIXED_CASE));
            assertThat(exception.getMessage(), containsString(
                    "object " + SCHEMA_EXASOL_TEST + "." + TABLE_POSTGRES_MIXED_CASE.toUpperCase() + " not found"));
        }

        @Test
        void testQueryLowerCaseTable() throws SQLException {
            final ResultSet result = statementExasol.executeQuery(
                    "SELECT x FROM " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE + "." + TABLE_POSTGRES_LOWER_CASE);
            assertThat(result.next(), equalTo(false));
        }

        @Test
        void testUnsetIgnoreUpperCaseTablesAndRefreshThrowsException() throws SQLException {
            statementExasol.execute(
                    "ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE + " set ignore_errors=''");
            statementExasol.execute("ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE
                    + " set POSTGRESQL_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER'");
            final DataException exception = assertThrows(DataException.class, () -> statementExasol
                    .execute("ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE + " REFRESH"));
            assertThat(exception.getMessage(), containsString("Table " + TABLE_POSTGRES_MIXED_CASE
                    + " cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation."));
        }

        @Test
        void testSetIgnoreUpperCaseTablesAndRefresh() throws SQLException {
            statementExasol.execute("ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE
                    + " set ignore_errors='POSTGRESQL_UPPERCASE_TABLES'");
            final String refresh_schema_query = "ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE
                    + " REFRESH";
            assertDoesNotThrow(() -> statementExasol.execute(refresh_schema_query));
        }
    }

    @Nested
    @DisplayName("Identifier Test - PRESERVE_ORIGINAL_CASE mode")
    class IdentifierPreserveOriginalCaseTest {
        @Test
        void testPreserveCaseQueryLowerCaseTableThrowsException() {
            final SQLException exception = assertThrows(SQLException.class,
                    () -> statementExasol.executeQuery("SELECT x FROM  "
                            + VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE + "." + TABLE_POSTGRES_LOWER_CASE));
            assertThat(exception.getMessage(), containsString("object " + VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE
                    + "." + TABLE_POSTGRES_LOWER_CASE.toUpperCase() + " not found"));
        }

        @Test
        void testPreserveCaseQueryLowerCaseTableWithQuotes() throws SQLException {
            final ResultSet result = statementExasol.executeQuery("SELECT \"x\" FROM  "
                    + VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE + ".\"" + TABLE_POSTGRES_LOWER_CASE + "\"");
            assertThat(result.next(), equalTo(false));
        }

        @Test
        void testPreserveCaseQueryUpperCaseTableWithQuotes() throws SQLException {
            final ResultSet result = statementExasol.executeQuery("SELECT \"Y\" FROM  "
                    + VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"");
            assertThat(result.next(), equalTo(false));
        }

        @Test
        void testPreserveCaseQueryUpperCaseTableWithQuotesLowerCaseColumn() throws SQLException {
            final ResultSet result = statementExasol.executeQuery("SELECT \"x\" FROM  "
                    + VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"");
            assertThat(result.next(), equalTo(false));
        }
    }

    @Nested
    @DisplayName("Datatype tests")
    class DatatypeTest {
        @Test
        void testDatatypeBigint() throws SQLException {
            assertSingleValue("myBigint", "DECIMAL(19,0)", "10000000000");
        }

        private void assertSingleValue(final String columnName, final String expectedColumnType,
                final String expectedValue) throws SQLException {
            final String qualifiedExpectedTableName = SCHEMA_EXASOL_TEST + "." + "EXPECTED";
            statementExasol.execute(
                    "CREATE OR REPLACE TABLE " + qualifiedExpectedTableName + "(x " + expectedColumnType + ")");
            statementExasol.execute("INSERT INTO " + qualifiedExpectedTableName + " VALUES(" + expectedValue + ")");
            final ResultSet expected = statementExasol.executeQuery("SELECT * FROM " + qualifiedExpectedTableName);
            final ResultSet actual = statementExasol.executeQuery(
                    "SELECT " + columnName + " FROM " + VIRTUAL_SCHEMA_POSTGRES + "." + TABLE_POSTGRES_ALL_DATA_TYPES);
            MatcherAssert.assertThat(actual, matchesResultSet(expected));
        }

        @Test
        void testDatatypeBigSerial() throws SQLException {
            assertSingleValue("myBigserial", "DECIMAL(19,0)", "1");
        }

        @Test
        void testDatatypeBit() throws SQLException {
            assertSingleValue("myBit", "BOOLEAN", "TRUE");
        }

        @Test
        void testDatatypeBitVar() throws SQLException {
            assertSingleValue("myBitvar", "VARCHAR(5) UTF8", "0");
        }

        @Test
        void testDatatypeBoolean() throws SQLException {
            assertSingleValue("myBoolean", "BOOLEAN", "FALSE");
        }

        @Test
        void testDatatypeBox() throws SQLException {
            assertSingleValue("myBox", "VARCHAR(2000000) UTF8", "'(4,16),(1,8)'");
        }

        @Test
        void testDatatypeBytea() throws SQLException {
            assertSingleValue("myBytea", "VARCHAR(2000000) UTF8", "'bytea NOT SUPPORTED'");
        }

        @Test
        void testDatatypeCharacter() throws SQLException {
            final String empty = " ";
            final String expected = "hajksdf" + String.join("", Collections.nCopies(993, empty));
            assertSingleValue("myCharacter", "CHAR(1000) ASCII", "'" + expected + "'");
        }

        @Test
        void testDatatypeCharacterVar() throws SQLException {
            assertSingleValue("myCharactervar", "VARCHAR(1000) ASCII", "'hjkdhjgfh'");
        }

        @Test
        void testDatatypeCidr() throws SQLException {
            assertSingleValue("myCidr", "VARCHAR(2000000) UTF8", "'192.168.100.128/25'");
        }

        @Test
        void testDatatypeCircle() throws SQLException {
            assertSingleValue("myCircle", "VARCHAR(2000000) UTF8", "'<(1,5),3>'");
        }

        @Test
        void testDatatypeDate() throws SQLException {
            assertSingleValue("myDate", "DATE", "'2010-01-01'");
        }

        @Test
        void testDatatypeDouble() throws SQLException {
            assertSingleValue("myDouble", "DOUBLE", "192189234.1723854");
        }

        @Test
        void testDatatypeInet() throws SQLException {
            assertSingleValue("myInet", "VARCHAR(2000000) UTF8", "'192.168.100.128/32'");
        }

        @Test
        void testDatatypeInteger() throws SQLException {
            assertSingleValue("myInteger", "DECIMAL(10,0)", "7189234");
        }

        @Test
        void testDatatypeIntervalYM() throws SQLException {
            assertSingleValue("myInterval", "VARCHAR(2000000) UTF8", "'1 year'");
        }

        @Test
        void testDatatypeJSON() throws SQLException {
            assertSingleValue("myJson", "VARCHAR(2000000) UTF8",
                    "'{\"bar\": \"baz\", \"balance\": 7.77, \"active\": false}'");
        }

        @Test
        void testDatatypeJSONB() throws SQLException {
            assertSingleValue("myJsonb", "VARCHAR(2000000) UTF8",
                    "'{\"bar\": \"baz\", \"active\": false, \"balance\": 7.77}'");
        }

        @Test
        void testDatatypeLine() throws SQLException {
            assertSingleValue("myLine", "VARCHAR(2000000) UTF8", "'{1,2,3}'");
        }

        @Test
        void testDatatypeLSeg() throws SQLException {
            assertSingleValue("myLseg", "VARCHAR(2000000) UTF8", "'[(1,2),(3,4)]'");
        }

        @Test
        void testDatatypeMACAddr() throws SQLException {
            assertSingleValue("myMacaddr", "VARCHAR(2000000) UTF8", "'08:00:2b:01:02:03'");
        }

        @Test
        void testDatatypeMoney() throws SQLException {
            assertSingleValue("myMoney", "DOUBLE", "100.01");
        }

        @Test
        void testDatatypeNumeric() throws SQLException {
            assertSingleValue("myNumeric", "VARCHAR(2000000) UTF8", "'24.2300000000'");
        }

        @Test
        void testDatatypePath() throws SQLException {
            assertSingleValue("myPath", "VARCHAR(2000000) UTF8", "'[(1,2),(3,4)]'");
        }

        @Test
        void testDatatypePoint() throws SQLException {
            assertSingleValue("myPoint", "VARCHAR(2000000) UTF8", "'(1,3)'");
        }

        @Test
        void testDatatypePolygon() throws SQLException {
            assertSingleValue("myPolygon", "VARCHAR(2000000) UTF8", "'((1,2),(2,4),(3,7))'");
        }

        @Test
        void testDatatypeReal() throws SQLException {
            assertSingleValue("myReal", "DOUBLE", "10.1199999");
        }

        @Test
        void testDatatypeSmallInt() throws SQLException {
            assertSingleValue("mySmallint", "DECIMAL(5,0)", "100");
        }

        @Test
        void testDatatypeText() throws SQLException {
            assertSingleValue("myText", "VARCHAR(2000000) ASCII", "'This cat is super cute'");
        }

        @Test
        void testDatatypeTime() throws SQLException {
            assertSingleValue("myTime", "VARCHAR(2000000) UTF8", "'1970-01-01 11:11:11.0'");
        }

        @Test
        void testDatatypeTimeWithTimezone() throws SQLException {
            assertSingleValue("myTimeWithTimeZone", "VARCHAR(2000000) UTF8", "'1970-01-01 11:11:11.0'");
        }

        @Test
        void testDatatypeTimestamp() throws SQLException {
            assertSingleValue("myTimestamp", "TIMESTAMP", "'2010-01-01 11:11:11.000000'");
        }

        @Test
        void testDatatypeTimestampWithTimezone() throws SQLException {
            assertSingleValue("myTimestampwithtimezone", "TIMESTAMP", "'2010-01-01 11:11:11.000000'");
        }

        @Test
        void testDatatypeTsQuery() throws SQLException {
            assertSingleValue("myTsquery", "VARCHAR(2000000) UTF8", "'''fat'' & ''rat'''");
        }

        @Test
        void testDatatypeTsvector() throws SQLException {
            assertSingleValue("myTsvector", "VARCHAR(2000000) UTF8", "'''fat'':2 ''rat'':3'");
        }

        @Test
        void testDatatypeUUID() throws SQLException {
            assertSingleValue("myUuid", "VARCHAR(2000000) UTF8", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
        }

        @Test
        void testDatatypeXML() throws SQLException {
            assertSingleValue("myXml", "VARCHAR(2000000) UTF8",
                    "'<?xml version=\"1.0\"?><book><title>Manual</title><chapter>...</chapter></book>'");
        }
    }
}