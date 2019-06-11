package com.exasol.adapter.dialects;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.Yaml;

public class IntegrationTestConfig {

    Map<String, Object> config;

    private static Pattern jdbcConnectionStringRegEx = Pattern.compile("[/@]+([^:/@]+)(:([0-9]+))?(/.*)?");

    public IntegrationTestConfig() throws FileNotFoundException {
        this(getMandatorySystemProperty("integrationtest.configfile"));
    }

    public IntegrationTestConfig(final String configFile) throws FileNotFoundException {
        try {
            this.config = loadConfig(configFile);
        } catch (final FileNotFoundException ex) {
            throw new FileNotFoundException(
                    "The specified integration test config file could not be found: " + configFile);
        } catch (final Exception ex) {
            throw new RuntimeException("The specified integration test config file could not be parsed: " + configFile,
                    ex);
        }
    }

    public String getJdbcAdapterPath() {
        return getProperty("general", "jdbcAdapterPath");
    }

    public String getScpTargetPath() {
        return getProperty("general", "scpTargetPath");
    }

    public boolean exasolTestsRequested() {
        return getProperty("exasol", "runIntegrationTests", false);
    }

    public String getExasolAddress() {
        return getProperty("exasol", "address");
    }

    public String getExasolUser() {
        return getProperty("exasol", "user");
    }

    public String getExasolPassword() {
        return getProperty("exasol", "password");
    }

    public boolean isDebugOn() {
        return getProperty("general", "debug", false);
    }

    public String debugAddress() {
        return getProperty("general", "debugAddress", "");
    }

    public boolean impalaTestsRequested() {
        return getProperty("impala", "runIntegrationTests", false);
    }

    public boolean hiveTestsRequested() {
        return getProperty("hive", "runIntegrationTests", false);
    }

    public String getImpalaJdbcConnectionString() {
        return getProperty("impala", "connectionString");
    }

    public String getImpalaJdbcPrefixPath() {
        return getProperty("impala", "jdbcDriverPath");
    }

    public String getHiveJdbcDriverPath() {
        return getProperty("hive", "jdbcDriverPath");
    }

    public List<String> getImpalaJdbcJars() {
        return getProperty("impala", "jdbcDriverJars");
    }

    public String getHiveJdbcConnectionString() {
        return getProperty("hive", "connectionString");
    }

    public String getHiveDockerJdbcConnectionString() {
        return getProperty("hive", "dockerConnectionString");
    }

    public boolean kerberosTestsRequested() {
        return getProperty("kerberos", "runIntegrationTests", false);
    }

    public String getKerberosJdbcConnectionString() {
        return getProperty("kerberos", "connectionString");
    }

    public String getKerberosJdbcPrefixPath() {
        return getProperty("kerberos", "jdbcDriverPath");
    }

    public String getKerberosUser() {
        return getProperty("kerberos", "user");
    }

    public String getKerberosPassword() {
        return getProperty("kerberos", "password");
    }

    public List<String> getKerberosJdbcJars() {
        return getProperty("kerberos", "jdbcDriverJars");
    }

    public boolean oracleTestsRequested() {
        return getProperty("oracle", "runIntegrationTests", false);
    }

    public String getOracleJdbcDriverPath() {
        return getProperty("oracle", "jdbcDriverPath");
    }

    public String getOracleDockerJdbcConnectionString() {
        return getProperty("oracle", "dockerConnectionString");
    }

    public String getOracleJdbcConnectionString() {
        return getProperty("oracle", "connectionString");
    }

    public URI getOracleConnectionInformation() {
        return getURIFor(getOracleJdbcConnectionString());
    }

    public URI getOracleDockerConnectionInformation() {
        return getURIFor(getOracleDockerJdbcConnectionString());
    }

    public URI getURIFor(final String connectionString) {
        final Matcher matcher = jdbcConnectionStringRegEx.matcher(connectionString);
        if (!matcher.find()) {
            throw new RuntimeException("oracle.connectionString '" + connectionString + "' could not be parsed");
        }

        final String host = matcher.group(1);
        final String portMatch = matcher.group(3);
        int port = -1;
        if (portMatch != null) {
            port = Integer.parseInt(portMatch);
        }
        if (port == -1) {
            port = 1521;
        }
        final String path = matcher.group(4);

        try {
            return new URI(null, null, host, port, path, null, null);
        } catch (final URISyntaxException e) {
            throw new RuntimeException(
                    "oracle.connectionString '" + connectionString + "' could not be parsed: " + e.getMessage());
        }
    }

    public String getOracleUser() {
        return getProperty("oracle", "user");
    }

    public String getOraclePassword() {
        return getProperty("oracle", "password");
    }

    public String getTeradataJdbcConnectionString() {
        return getProperty("teradata", "connectionString");
    }

    public String getTeradataUser() {
        return getProperty("teradata", "user");
    }

    public String getTeradataPassword() {
        return getProperty("teradata", "password");
    }

    public String getTeradataJdbcPrefixPath() {
        return getProperty("teradata", "jdbcDriverPath");
    }

    public List<String> getTeradataJdbcJars() {
        return getProperty("teradata", "jdbcDriverJars");
    }

    public boolean teradataTestsRequested() {
        return getProperty("teradata", "runIntegrationTests", false);
    }

    public String getDB2JdbcConnectionString() {
        return getProperty("db2", "connectionString");
    }

    public String getDB2User() {
        return getProperty("db2", "user");
    }

    public String getDB2Password() {
        return getProperty("db2", "password");
    }

    public String getDB2JdbcPrefixPath() {
        return getProperty("db2", "jdbcDriverPath");
    }

    public List<String> getDB2JdbcJars() {
        return getProperty("db2", "jdbcDriverJars");
    }

    public boolean DB2TestsRequested() {
        return getProperty("db2", "runIntegrationTests", false);
    }

    public boolean genericTestsRequested() {
        return getProperty("generic", "runIntegrationTests", false);
    }

    public String getGenericJdbcDriverPath() {
        return getProperty("generic", "jdbcDriverPath");
    }

    public String getGenericJdbcConnectionString() {
        return getProperty("generic", "connectionString");
    }

    public String getGenericUser() {
        return getProperty("generic", "user");
    }

    public String getGenericPassword() {
        return getProperty("generic", "password");
    }

    public boolean sybaseTestsRequested() {
        return getProperty("sybase", "runIntegrationTests", false);
    }

    public String getSybaseJdbcDriverPath() {
        return getProperty("sybase", "jdbcDriverPath");
    }

    public String getSybaseJdbcConnectionString() {
        return getProperty("sybase", "connectionString");
    }

    public String getSybaseUser() {
        return getProperty("sybase", "user");
    }

    public String getSybasePassword() {
        return getProperty("sybase", "password");
    }

    public boolean getPostgresqlTestsRequested() {
        return getProperty("postgresql", "runIntegrationTests", false);
    }

    public String getPostgresqlJdbcDriverPath() {
        return getProperty("postgresql", "jdbcDriverPath");
    }

    public String getPostgresqlJdbcConnectionString() {
        return getProperty("postgresql", "connectionString");
    }

    public String getPostgresqlDockerJdbcConnectionString() {
        return getProperty("postgresql", "dockerConnectionString");
    }

    public String getPostgresqlUser() {
        return getProperty("postgresql", "user");
    }

    public String getPostgresqlPassword() {
        return getProperty("postgresql", "password");
    }

    public String getBucketFSPassword() {
        return getProperty("general", "bucketFsPassword");
    }

    public String getBucketFSURL() {
        return getProperty("general", "bucketFsUrl");
    }

    private Map<String, Object> loadConfig(final String configFile) throws FileNotFoundException {
        final Yaml yaml = new Yaml();
        final File file = new File(configFile);
        InputStream inputStream = null;
        inputStream = new FileInputStream(file);
        @SuppressWarnings("unchecked")
        final Map<String, Object> configuration = (Map<String, Object>) yaml.load(inputStream);
        return configuration;
    }

    private <T> T getProperty(final String section, final String key, final T defaultValue) {
        try {
            return getProperty(section, key);
        } catch (final Exception ex) {
            return defaultValue;
        }
    }

    private <T> T getProperty(final String section, final String key) {
        if (!this.config.containsKey(section)) {
            throw new RuntimeException("Integration test config file has no section '" + section + "'");
        }
        @SuppressWarnings("unchecked")
        final Map<String, Object> sectionMap = (Map<String, Object>) this.config.get(section);
        if (!sectionMap.containsKey(key)) {
            throw new RuntimeException(
                    "Integration test config file has no key '" + key + "' in section '" + section + "'");
        }
        @SuppressWarnings("unchecked")
        final T property = (T) sectionMap.get(key);
        return property;
    }

    private static String getMandatorySystemProperty(final String propertyName) {
        final String value = System.getProperty(propertyName);
        if (value == null) {
            throw new RuntimeException("Integration tests requires system property '" + propertyName + "' to be set.");
        }
        return value;
    }
}
