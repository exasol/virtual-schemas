package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;
import static com.exasol.dbbuilder.dialects.exasol.AdapterScript.Language.JAVA;
import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import com.exasol.dbbuilder.dialects.DatabaseObjectException;
import com.exasol.dbbuilder.dialects.exasol.*;

@Tag("integration")
@Testcontainers
class PostgreSQLSqlDialectIT extends AbstractIntegrationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSqlDialectIT.class);
    private static final String POSTGRES_DRIVER_NAME_AND_VERSION = "postgresql-42.2.5.jar";
    private static final Path PATH_TO_POSTGRES_DRIVER = Path.of("src", "test", "resources", "integration", "driver",
            "postgres", POSTGRES_DRIVER_NAME_AND_VERSION);
    private static final String POSTGRES_CONTAINER_NAME = "postgres:9.6.2";
    private static final String JDBC_CONNECTION_NAME = "JDBC";
    private static final String SCHEMA_POSTGRES = "schema_postgres";
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
    private static final int POSTGRES_PORT = 5432;

    @Container
    private static final PostgreSQLContainer<? extends PostgreSQLContainer<?>> postgresqlContainer = new PostgreSQLContainer<>(
            POSTGRES_CONTAINER_NAME);
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> exasolContainer = new ExasolContainer<>(
            EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static Statement statementExasol;
    private static ExasolObjectFactory exasolFactory;
    private static ConnectionDefinition connectionDefinition;
    private static AdapterScript adapterScript;

    @BeforeAll
    static void beforeAll() throws InterruptedException, BucketAccessException, TimeoutException, SQLException {
        final Bucket bucket = exasolContainer.getDefaultBucket();
        bucket.uploadFile(PATH_TO_POSTGRES_DRIVER, POSTGRES_DRIVER_NAME_AND_VERSION);
        uploadVsJarToBucket(bucket);
        try (final Connection postgresConnection = postgresqlContainer.createConnection("")) {
            final Statement statementPostgres = postgresConnection.createStatement();
            statementPostgres.execute("CREATE SCHEMA " + SCHEMA_POSTGRES);
            statementPostgres.execute("CREATE SCHEMA " + SCHEMA_POSTGRES_UPPERCASE_TABLE);
            createPostgresTestTableSimple(statementPostgres);
            createPostgresTestTableAllDataTypes(statementPostgres);
            createPostgresTestTableMixedCase(statementPostgres);
            createPostgresTestTableLowerCase(statementPostgres);
            createTestTablesForJoinTests(postgresConnection, SCHEMA_POSTGRES);
        }
        statementExasol = exasolContainer.createConnection("").createStatement();
        exasolFactory = new ExasolObjectFactory(exasolContainer.createConnection(""));
        final ExasolSchema exasolSchema = exasolFactory.createSchema(SCHEMA_EXASOL);
        adapterScript = createAdapterScript(exasolSchema);
        final String connectionString = "jdbc:postgresql://" + DOCKER_IP_ADDRESS + ":"
                + postgresqlContainer.getMappedPort(POSTGRES_PORT) + "/" + postgresqlContainer.getDatabaseName();
        connectionDefinition = exasolFactory.createConnectionDefinition(JDBC_CONNECTION_NAME, connectionString,
                postgresqlContainer.getUsername(), postgresqlContainer.getPassword());
        exasolFactory.createVirtualSchemaBuilder(VIRTUAL_SCHEMA_POSTGRES).adapterScript(adapterScript)
                .connectionDefinition(connectionDefinition).dialectName("POSTGRESQL")
                .properties(Map.of("CATALOG_NAME", postgresqlContainer.getDatabaseName(), //
                        "SCHEMA_NAME", SCHEMA_POSTGRES))
                .build();
        exasolFactory.createVirtualSchemaBuilder(VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE).adapterScript(adapterScript)
                .connectionDefinition(connectionDefinition).dialectName("POSTGRESQL")
                .properties(Map.of("CATALOG_NAME", postgresqlContainer.getDatabaseName(), //
                        "SCHEMA_NAME", SCHEMA_POSTGRES_UPPERCASE_TABLE, //
                        "IGNORE_ERRORS", "POSTGRESQL_UPPERCASE_TABLES"))
                .build();
        exasolFactory.createVirtualSchemaBuilder(VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE)
                .adapterScript(adapterScript).connectionDefinition(connectionDefinition).dialectName("POSTGRESQL")
                .properties(Map.of("CATALOG_NAME", postgresqlContainer.getDatabaseName(), //
                        "SCHEMA_NAME", SCHEMA_POSTGRES_UPPERCASE_TABLE, //
                        "POSTGRESQL_IDENTIFIER_MAPPING", "PRESERVE_ORIGINAL_CASE"))
                .build();
    }

    private static void createPostgresTestTableSimple(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES + "." + TABLE_POSTGRES_SIMPLE;
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT)");
        statementPostgres.execute("INSERT INTO " + qualifiedTableName + " VALUES (1)");
    }

    private static void createPostgresTestTableAllDataTypes(final Statement statementPostgres) throws SQLException {
        final String qualifiedTableName = SCHEMA_POSTGRES + "." + TABLE_POSTGRES_ALL_DATA_TYPES;
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

    private static AdapterScript createAdapterScript(final ExasolSchema schema) {
        final String content = "%scriptclass com.exasol.adapter.RequestDispatcher;\n" //
                + "%jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n";
        return schema.createAdapterScript(ADAPTER_SCRIPT_EXASOL, JAVA, content);
    }

    @Test
    void testSelectSingleColumn() throws SQLException {
        final String qualifiedExpectedTableName = SCHEMA_EXASOL + ".TABLE_POSTGRES_SIMPLE_EXPECTED";
        statementExasol.execute("CREATE OR REPLACE TABLE " + qualifiedExpectedTableName + " (x INT)");
        statementExasol.execute("INSERT INTO " + qualifiedExpectedTableName + " VALUES (1)");
        final ResultSet expectedResultSet = statementExasol.executeQuery("SELECT * FROM " + qualifiedExpectedTableName);
        final ResultSet actualResultSet = statementExasol
                .executeQuery("SELECT * FROM " + VIRTUAL_SCHEMA_POSTGRES + "." + TABLE_POSTGRES_SIMPLE);
        assertThat(actualResultSet, matchesResultSet(expectedResultSet));
    }

    @Test
    void testCountAll() throws SQLException {
        final String qualifiedExpectedTableName = VIRTUAL_SCHEMA_POSTGRES + "." + TABLE_POSTGRES_SIMPLE;
        final String query = "SELECT COUNT(*) FROM " + qualifiedExpectedTableName;
        final ResultSet expected = getExpectedResultSet(List.of("x DECIMAL(19,0)"), //
                List.of("1.00000"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testInnerJoin() throws SQLException {
        final String query = "SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " a INNER JOIN  "
                + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x";
        final ResultSet expected = getExpectedResultSet(List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                List.of("2,'bbb', 2,'bbb'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testInnerJoinWithProjection() throws SQLException {
        final String query = "SELECT b.y || " + QUALIFIED_TABLE_JOIN_NAME_1 + ".y FROM " + QUALIFIED_TABLE_JOIN_NAME_1
                + " INNER JOIN  " + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON " + QUALIFIED_TABLE_JOIN_NAME_1 + ".x=b.x";
        final ResultSet expected = getExpectedResultSet(List.of("y VARCHAR(100)"), //
                List.of("'bbbbbb'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testLeftJoin() throws SQLException {
        final String query = "SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " a LEFT OUTER JOIN  "
                + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x ORDER BY a.x";
        final ResultSet expected = getExpectedResultSet(List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                List.of("1, 'aaa', null, null", //
                        "2, 'bbb', 2, 'bbb'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testRightJoin() throws SQLException {
        final String query = "SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " a RIGHT OUTER JOIN  "
                + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x ORDER BY a.x";
        final ResultSet expected = getExpectedResultSet(List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                List.of("2, 'bbb', 2, 'bbb'", //
                        "null, null, 3, 'ccc'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testFullOuterJoin() throws SQLException {
        final String query = "SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " a FULL OUTER JOIN  "
                + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x=b.x ORDER BY a.x";
        final ResultSet expected = getExpectedResultSet(List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                List.of("1, 'aaa', null, null", //
                        "2, 'bbb', 2, 'bbb'", //
                        "null, null, 3, 'ccc'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testRightJoinWithComplexCondition() throws SQLException {
        final String query = "SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " a RIGHT OUTER JOIN  "
                + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x||a.y=b.x||b.y ORDER BY a.x";
        final ResultSet expected = getExpectedResultSet(List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                List.of("2, 'bbb', 2, 'bbb'", //
                        "null, null, 3, 'ccc'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testFullOuterJoinWithComplexCondition() throws SQLException {
        final String query = "SELECT * FROM " + QUALIFIED_TABLE_JOIN_NAME_1 + " a FULL OUTER JOIN  "
                + QUALIFIED_TABLE_JOIN_NAME_2 + " b ON a.x-b.x=0 ORDER BY a.x";
        final ResultSet expected = getExpectedResultSet(List.of("x INT", "y VARCHAR(100)", "a INT", "b VARCHAR(100)"), //
                List.of("1, 'aaa', null, null", //
                        "2, 'bbb', 2, 'bbb'", //
                        "null, null, 3, 'ccc'"));
        assertThat(getActualResultSet(query), matchesResultSet(expected));
    }

    @Test
    void testCreateSchemaWithUpperCaseTablesThrowsException() {
        final VirtualSchema.Builder builder = exasolFactory.createVirtualSchemaBuilder("WRONG_VIRTUAL_SCHEMA")
                .adapterScript(adapterScript).connectionDefinition(connectionDefinition).dialectName("POSTGRESQL")
                .properties(Map.of("CATALOG_NAME", postgresqlContainer.getDatabaseName(), //
                        "SCHEMA_NAME", SCHEMA_POSTGRES_UPPERCASE_TABLE));
        final Exception exception = assertThrows(DatabaseObjectException.class, builder::build);
        assertThat(exception.getMessage(), containsString("Failed to write to object"));
    }

    @Test
    void testQueryUpperCaseTableQuotedThrowsException() {
        final Exception exception = assertThrows(SQLException.class, () -> statementExasol
                .execute("SELECT x FROM  " + SCHEMA_EXASOL + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\""));
        assertThat(exception.getMessage(),
                containsString("object \"" + SCHEMA_EXASOL + "\".\"" + TABLE_POSTGRES_MIXED_CASE + "\" not found"));
    }

    @Test
    void testQueryUpperCaseTableThrowsException() {
        final Exception exception = assertThrows(SQLException.class,
                () -> statementExasol.execute("SELECT x FROM  " + SCHEMA_EXASOL + "." + TABLE_POSTGRES_MIXED_CASE));
        assertThat(exception.getMessage(), containsString(
                "object " + SCHEMA_EXASOL + "." + TABLE_POSTGRES_MIXED_CASE.toUpperCase() + " not found"));
    }

    @Test
    void testQueryLowerCaseTable() throws SQLException {
        final ResultSet result = statementExasol.executeQuery(
                "SELECT x FROM " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE + "." + TABLE_POSTGRES_LOWER_CASE);
        assertThat(result.next(), equalTo(false));
    }

    @Test
    void testUnsetIgnoreUpperCaseTablesAndRefreshThrowsException() throws SQLException {
        statementExasol
                .execute("ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE + " set ignore_errors=''");
        statementExasol.execute("ALTER VIRTUAL SCHEMA " + VIRTUAL_SCHEMA_POSTGRES_UPPERCASE_TABLE
                + " set POSTGRESQL_IDENTIFIER_MAPPING = 'CONVERT_TO_UPPER'");
        final Exception exception = assertThrows(SQLException.class, () -> statementExasol
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

    @Test
    void testPreserveCaseQueryLowerCaseTableThrowsException() {
        final SQLException exception = assertThrows(SQLException.class, () -> statementExasol.executeQuery(
                "SELECT x FROM  " + VIRTUAL_SCHEMA_POSTGRES_PRESERVE_ORIGINAL_CASE + "." + TABLE_POSTGRES_LOWER_CASE));
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

    @Test
    void testDatatypeBigint() throws SQLException {
        assertSingleValue("myBigint", "DECIMAL(19,0)", "10000000000");
    }

    private void assertSingleValue(final String columnName, final String expectedColumnType, final String expectedValue)
            throws SQLException {
        final String qualifiedExpectedTableName = SCHEMA_EXASOL + "." + "EXPECTED";
        statementExasol
                .execute("CREATE OR REPLACE TABLE " + qualifiedExpectedTableName + "(x " + expectedColumnType + ")");
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

    @Override
    protected Connection getExasolConnection() throws SQLException {
        return exasolContainer.createConnection("");
    }
}