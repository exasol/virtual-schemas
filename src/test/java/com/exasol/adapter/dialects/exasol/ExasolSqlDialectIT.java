package com.exasol.adapter.dialects.exasol;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.exasol.matcher.ResultSetMatcher.matchesResultSet;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag("integration")
@Testcontainers
class ExasolSqlDialectIT {
    private static final String VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION = "virtualschema-jdbc-adapter-dist-3.0.1.jar";
    private static final String VIRTUAL_SCHEMA_EXASOL_JDBC_NAME = "VIRTUAL_SCHEMA_EXASOL_JDBC";
    private static final String VIRTUAL_SCHEMA_EXASOL_JDBC_LOCAL_NAME = "VIRTUAL_SCHEMA_EXASOL_JDBC_LOCAL";
    private static final String VIRTUAL_SCHEMA_EXASOL_EXA_NAME = "VIRTUAL_SCHEMA_EXASOL_EXA";
    private static final String VIRTUAL_SCHEMA_EXASOL_EXA_LOCAL_NAME = "VIRTUAL_SCHEMA_EXASOL_EXA_LOCAL";
    public static final String SCHEMA_NAME = "TEST_SCHEMA";
    private static final String TABLE_ALL_EXASOL_DATA_TYPES = "ALL_EXASOL_TYPES";
    private static final String TABLE_WITH_NULLS = "WITH_NULLS";
    private static final String TABLE_SIMPLE_VALUES = "SIMPLE_VALUES";

    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> container = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE).withClusterLogsPath(Path.of("target/mylogs"));
    private static Statement statement;

    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException {
        final Bucket bucket = container.getDefaultBucket();
        final Path pathToRls = Path.of("target/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        bucket.uploadFile(pathToRls, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        TimeUnit.SECONDS.sleep(20);
        final Connection connection = container.createConnectionForUser(container.getUsername(),
                container.getPassword());
        statement = connection.createStatement();
        createTestSchema(SCHEMA_NAME);
        createTestTableAllExasolDataTypes();
        createTestTableWithNulls();
        createTestTableWithSimpleValues();
        createConnection();
        createAdapterScript();
        createVirtualSchema(VIRTUAL_SCHEMA_EXASOL_JDBC_NAME, Optional.empty());
    }

    private static void createTestTableWithSimpleValues() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + SCHEMA_NAME + "." + TABLE_SIMPLE_VALUES //
                + "(a INT, " //
                + "b VARCHAR(100), " //
                + "c DOUBLE)");
        statement.execute("INSERT INTO " + SCHEMA_NAME + "." + TABLE_SIMPLE_VALUES + " VALUES " //
                + " (1, 'a', 1.1), " //
                + "(2, 'b', 2.2), " //
                + "(3, 'c', 3.3), " //
                + "(1, 'd', 4.4), " //
                + "(2, 'e', 5.5), " //
                + "(3, 'f', 6.6), " //
                + "(null, null, null)");
    }

    private static void createTestTableWithNulls() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + SCHEMA_NAME + "." + TABLE_WITH_NULLS //
                + "(c1 INT, " //
                + "c2 VARCHAR(100))");
        statement.execute("INSERT INTO " + SCHEMA_NAME + "." + TABLE_WITH_NULLS + " VALUES " //
                + "(1, 'a'), " //
                + "(2, null), " //
                + "(3, 'b'), " //
                + "(1, null), " //
                + "(null, 'c')");
    }

    private static void createTestTableAllExasolDataTypes() throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + SCHEMA_NAME + "." + TABLE_ALL_EXASOL_DATA_TYPES //
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
        statement.execute("INSERT INTO " + SCHEMA_NAME + "." + TABLE_ALL_EXASOL_DATA_TYPES + " VALUES " //
                + "('a茶', 'b', 'c茶', 'd', 123, 123.456, 2.2, FALSE, '2016-08-01', '2016-08-01 00:00:01.000', " //
                + "'2016-08-01 00:00:02.000', '4-6', '3 12:50:10.123', 'POINT(2 5)')");
    }

    private static void createTestSchema(final String schemaName) throws SQLException {
        statement.execute("CREATE SCHEMA " + schemaName);
        statement.execute("OPEN SCHEMA " + schemaName);
    }

    private static void createConnection() throws SQLException {
        statement.execute("CREATE CONNECTION JDBC_EXASOL_CONNECTION " //
                + "TO 'jdbc:exa:localhost:8888' " //
                + "USER '" + container.getUsername() + "' " //
                + "IDENTIFIED BY '" + container.getPassword() + "'");
    }

    private static void createAdapterScript() throws SQLException, InterruptedException {
        statement.execute("CREATE OR REPLACE JAVA ADAPTER SCRIPT " + SCHEMA_NAME + ".ADAPTER_SCRIPT_EXASOL AS " //
                + "%scriptclass com.exasol.adapter.RequestDispatcher;\n" //
                + "%jar /buckets/bfsdefault/default/" + VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION + ";\n" //
                + "/");
        TimeUnit.SECONDS.sleep(20); // FIXME: need to be fixed in the container
    }

    private static void createVirtualSchema(final String virtualSchemaName, final Optional<String> additionalParameters)
            throws SQLException {
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE VIRTUAL SCHEMA ");
        builder.append(virtualSchemaName);
        builder.append(" USING " + SCHEMA_NAME + ".ADAPTER_SCRIPT_EXASOL WITH ");
        builder.append("SQL_DIALECT     = 'EXASOL' ");
        builder.append("CONNECTION_NAME = 'JDBC_EXASOL_CONNECTION' ");
        builder.append("SCHEMA_NAME     = '" + SCHEMA_NAME + "' ");
        additionalParameters.ifPresent(builder::append);
        statement.execute(builder.toString());
    }

    @Test
    void testDataTypeMapping() throws SQLException {
        final String expectedSchemaQualifiedTableName = SCHEMA_NAME + ".EXA_DBA_COLUMNS_EXPECTED";
        statement.execute("CREATE OR REPLACE TABLE " + expectedSchemaQualifiedTableName //
                + "(COLUMN_NAME VARCHAR(128), COLUMN_TYPE VARCHAR(40), COLUMN_MAXSIZE DECIMAL(18,0), COLUMN_NUM_PREC DECIMAL(18, 0), COLUMN_NUM_SCALE DECIMAL(18, 0), COLUMN_DEFAULT VARCHAR(2000))");
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
        final ResultSet actualResultSet = statement.executeQuery(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '"
                        + SCHEMA_NAME + "' AND COLUMN_TABLE='" + TABLE_ALL_EXASOL_DATA_TYPES + "' ORDER BY COLUMN_ORDINAL_POSITION");
        final ResultSet expectedResultSet = statement.executeQuery("SELECT * FROM " + expectedSchemaQualifiedTableName);
        assertThat(actualResultSet, matchesResultSet(expectedResultSet));
    }
}