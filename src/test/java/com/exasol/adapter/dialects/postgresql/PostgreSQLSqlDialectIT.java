package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.IntegrationTestsConstants.*;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import utils.IntegrationTestSetupManager;

@Tag("integration")
@Testcontainers
class PostgreSQLSqlDialectIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSqlDialectIT.class);
    private static final String POSTGRES_DRIVER_NAME_AND_VERSION = "postgresql-42.2.5.jar";
    public static final Path PATH_TO_POSTGRES_DRIVER = Path
            .of("src/test/resources/integration/driver/" + POSTGRES_DRIVER_NAME_AND_VERSION);
    private static final String POSTGRES_DOCKER_VERSION = "postgres:9.6.2";
    private static final String SCHEMA_POSTGRES_TEST = "SCHEMA_POSTGRES_TEST";
    private static final String SCHEMA_POSTGRES_MIXED_CASE_TEST = "SCHEMA_POSTGRES_MIXED_CASE_TEST";
    private static final String TABLE_POSTGRES_MIXED_CASE = "Table_Postgres_Mixed_Case";
    private static final String TABLE_POSTGRES_SIMPLE = "TABLE_POSTGRES_SIMPLE";
    private static final String TABLE_POSTGRES_ALL_DATA_TYPES = "TABLE_POSTGRES_ALL_DATA_TYPES";
    private static final String CONNECTION_POSTGRES_JDBC = "CONNECTION_POSTGRES_JDBC";
    private static final String VIRTUAL_SCHEMA_POSTGRES_JDBC = "VIRTUAL_SCHEMA_POSTGRES_JDBC";
    private static final String VIRTUAL_SCHEMA_POSTGRES_JDBC_UPPERCASE_TABLE = "VIRTUAL_SCHEMA_POSTGRES_JDBC_UPPERCASE_TABLE";
    private static final String VIRTUAL_SCHEMA_POSTGRES_JDBC_PRESERVE_ORIGINAL_CASE = "VIRTUAL_SCHEMA_POSTGRES_JDBC_PRESERVE_ORIGINAL_CASE";

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
        integrationTestSetupManager.createTestSchema(statementPostgres, SCHEMA_POSTGRES_MIXED_CASE_TEST);
        createPostgresTestTableSimple(statementPostgres);
        createPostgresTestTableAllDataTypes(statementPostgres);
        createPostgresTestTableMixedCase(statementPostgres);

        final String connectionString = "jdbc:postgresql://172.17.0.1:" + postgresqlContainer.getMappedPort(5432) + "/"
                + postgresqlContainer.getDatabaseName();
        integrationTestSetupManager.createConnection(statementExasol, CONNECTION_POSTGRES_JDBC, connectionString,
                postgresqlContainer.getUsername(), postgresqlContainer.getPassword());
        integrationTestSetupManager.createAdapterScript(statementExasol,
                SCHEMA_EXASOL_TEST + "." + ADAPTER_SCRIPT_EXASOL, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION,
                Optional.of("%jar /buckets/bfsdefault/default/" + POSTGRES_DRIVER_NAME_AND_VERSION + ";\n"));
        createVirtualSchema(VIRTUAL_SCHEMA_POSTGRES_JDBC, SCHEMA_EXASOL_TEST, Optional.empty());
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
        final HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        return dataSource;
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
        final String qualifiedTableName = SCHEMA_POSTGRES_MIXED_CASE_TEST + ".\"" + TABLE_POSTGRES_MIXED_CASE + "\"";
        statementPostgres.execute("CREATE TABLE " + qualifiedTableName + " (x INT, \"Y\" INT)");
    }

    private static void createVirtualSchema(final String virtualSchemaName, final String schemaName,
            final Optional<String> additionalParameters) throws SQLException {
        statementExasol.execute("OPEN SCHEMA " + "\"" + schemaName + "\"");
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE VIRTUAL SCHEMA ");
        builder.append(virtualSchemaName);
        builder.append(" USING " + SCHEMA_EXASOL_TEST + "." + ADAPTER_SCRIPT_EXASOL + " WITH ");
        builder.append("SQL_DIALECT     = 'POSTGRESQL' ");
        builder.append("CATALOG_NAME     = '" + postgresqlContainer.getDatabaseName() + "' ");
        builder.append("CONNECTION_NAME = '" + CONNECTION_POSTGRES_JDBC + "' ");
        builder.append("SCHEMA_NAME     = '" + schemaName + "' ");
        additionalParameters.ifPresent(builder::append);
        statementExasol.execute(builder.toString());
    }

    @Test
    void test() {
        assertTrue(postgresqlContainer.isRunning());
        assertTrue(exasolContainer.isRunning());
    }
}
