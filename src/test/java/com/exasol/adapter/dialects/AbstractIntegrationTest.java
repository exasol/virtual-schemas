package com.exasol.adapter.dialects;

import static com.exasol.adapter.dialects.IntegrationTestConstants.*;

import java.io.*;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;

public abstract class AbstractIntegrationTest {
    protected static void uploadDriverToBucket(final String driverName, final String resourcesDialectName,
            final Bucket bucket) throws InterruptedException, BucketAccessException, TimeoutException {
        final Path pathToSettingsFile = Path.of("src", "test", "resources", "integration", "driver",
                resourcesDialectName, JDBC_DRIVER_CONFIGURATION_FILE_NAME);
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
        bucket.uploadFile(pathToSettingsFile, "drivers/jdbc/" + JDBC_DRIVER_CONFIGURATION_FILE_NAME);
        final String driverPath = getPropertyFromFile(resourcesDialectName, "driver.path");
        bucket.uploadFile(Path.of(driverPath, driverName), "drivers/jdbc/" + driverName);
    }

    private static String getPathToPropertyFile(final String resourcesDialectName) {
        return "src/test/resources/integration/driver/" + resourcesDialectName + "/" + resourcesDialectName
                + ".properties";
    }

    protected static void uploadVsJarToBucket(final Bucket bucket)
            throws InterruptedException, BucketAccessException, TimeoutException {
        bucket.uploadFile(PATH_TO_VIRTUAL_SCHEMAS_JAR, VIRTUAL_SCHEMAS_JAR_NAME_AND_VERSION);
    }

    protected static String getPropertyFromFile(final String resourcesDialectName, final String propertyName) {
        final String pathToPropertyFile = getPathToPropertyFile(resourcesDialectName);
        try (final InputStream inputStream = new FileInputStream(pathToPropertyFile)) {
            final Properties properties = new Properties();
            properties.load(inputStream);
            return properties.getProperty(propertyName);
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    "Cannot access the properties file or read from it. Check if the path spelling is correct"
                            + " and if the file exists.");
        }
    }

    protected static void createTestTablesForJoinTests(final Connection connection, final String schemaName)
            throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + schemaName + "." + TABLE_JOIN_1 + "(x INT, y VARCHAR(100))");
            statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_1 + " VALUES (1,'aaa')");
            statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_1 + " VALUES (2,'bbb')");
            statement.execute("CREATE TABLE " + schemaName + "." + TABLE_JOIN_2 + "(x INT, y VARCHAR(100))");
            statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_2 + " VALUES (2,'bbb')");
            statement.execute("INSERT INTO " + schemaName + "." + TABLE_JOIN_2 + " VALUES (3,'ccc')");
        }
    }

    protected ResultSet getExpectedResultSet(final List<String> expectedColumns, final List<String> expectedRows)
            throws SQLException {
        final Connection connection = getExasolConnection();
        try (final Statement statement = connection.createStatement()) {
            final String expectedValues = expectedRows.stream().map(row -> "(" + row + ")")
                    .collect(Collectors.joining(","));
            final String qualifiedExpectedTableName = SCHEMA_EXASOL + ".EXPECTED";
            statement.execute("CREATE OR REPLACE TABLE " + qualifiedExpectedTableName + "("
                    + String.join(", ", expectedColumns) + ")");
            statement.execute("INSERT INTO " + qualifiedExpectedTableName + " VALUES" + expectedValues);
            return statement.executeQuery("SELECT * FROM " + qualifiedExpectedTableName);
        }
    }

    protected ResultSet getActualResultSet(final String query) throws SQLException {
        final Connection connection = getExasolConnection();
        try (final Statement statement = connection.createStatement()) {
            return statement.executeQuery(query);
        }
    }

    protected String getExplainVirtualString(final String query) throws SQLException {
        final Connection connection = getExasolConnection();
        try (final Statement statement = connection.createStatement()) {
            final ResultSet explainVirtual = statement.executeQuery("EXPLAIN VIRTUAL " + query);
            explainVirtual.next();
            return explainVirtual.getString("PUSHDOWN_SQL");
        }
    }

    protected abstract Connection getExasolConnection() throws SQLException;
}