package com.exasol.adapter.jdbc;

import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.exasol.*;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.metadata.DataType;

/**
 * Class to expose a nice interface to properties. Casts to the correct data types, checks for valid property values and
 * consistency.
 */
@Deprecated
public final class JdbcAdapterProperties {
    static final Logger LOGGER = Logger.getLogger(JdbcAdapterProperties.class.getName());
    static final String PROP_CONNECTION_NAME = "CONNECTION_NAME";
    // Optional Parameters
    static final String PROP_TABLES = "TABLE_FILTER";
    static final String PROP_IS_LOCAL = "IS_LOCAL";
    static final String PROP_SQL_DIALECT = "SQL_DIALECT";
    static final String PROP_IMPORT_FROM_EXA = "IMPORT_FROM_EXA";
    static final String PROP_EXA_CONNECTION_STRING = "EXA_CONNECTION_STRING";
    static final String PROP_IMPORT_FROM_ORA = "IMPORT_FROM_ORA";
    static final String PROP_ORA_CONNECTION_NAME = "ORA_CONNECTION_NAME";
    static final String PROP_EXCLUDED_CAPABILITIES = "EXCLUDED_CAPABILITIES";
    static final String PROP_ORACLE_CAST_NUMBER_TO_DECIMAL = "ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE";

    private JdbcAdapterProperties() {
        // prevent instantiation of static helper class
    }

    // Specifies different exception handling strategies
    public enum ExceptionHandlingMode {
        IGNORE_INVALID_VIEWS, NONE
    }

    private static String getProperty(final Map<String, String> properties, final String name,
            final String defaultValue) {
        return properties.getOrDefault(name, defaultValue);
    }

    private static String getProperty(final Map<String, String> properties, final String name) {
        return getProperty(properties, name, "");
    }

    public static DataType getOracleNumberTargetType(final Map<String, String> properties) {
        final String precisionAndScale = getProperty(properties, PROP_ORACLE_CAST_NUMBER_TO_DECIMAL);
        if (precisionAndScale.trim().isEmpty()) {
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
        final List<String> precisionAndScaleList = Arrays.stream(precisionAndScale.split(",")).map(String::trim)
                .collect(Collectors.toList());
        return DataType.createDecimal(Integer.valueOf(precisionAndScaleList.get(0)),
                Integer.valueOf(precisionAndScaleList.get(1)));
    }

    public static boolean isUserSpecifiedConnection(final Map<String, String> properties) {
        final String connName = getProperty(properties, PROP_CONNECTION_NAME);
        return ((connName != null) && !connName.isEmpty());
    }

    public static String getConnectionName(final Map<String, String> properties) {
        final String connName = getProperty(properties, PROP_CONNECTION_NAME);
        assert ((connName != null) && !connName.isEmpty());
        return connName;
    }

    /**
     * Returns the credentials for the remote system. These are either directly specified in the properties or obtained
     * from a connection (requires privilege to access the connection) .
     * 
     * @param properties     user-defined adapter properties
     * @param exasolMetadata Exasol metadata
     * @return Exasol connection information
     */
    public static ExaConnectionInformation getConnectionInformation(final AdapterProperties properties,
            final ExaMetadata exasolMetadata) {
        final String connectionName = properties.getConnectionName();
        if ((connectionName != null) && !connectionName.isEmpty()) {
            try {
                return exasolMetadata.getConnection(connectionName);
            } catch (final ExaConnectionAccessException exception) {
                throw new RuntimeException(
                        "Could not access the connection information of connection \"" + connectionName + "\"",
                        exception);
            }
        } else {
            return new ExaConnectionInformationJdbc(properties.getConnectionString(), properties.getUsername(),
                    properties.getPassword());
        }
    }

    public static boolean isImportFromExa(final Map<String, String> properties) {
        return getProperty(properties, PROP_IMPORT_FROM_EXA).toUpperCase().equals("TRUE");
    }

    public static boolean isImportFromOra(final Map<String, String> properties) {
        return getProperty(properties, PROP_IMPORT_FROM_ORA).toUpperCase().equals("TRUE");
    }

    public static String getExaConnectionString(final Map<String, String> properties) {
        return getProperty(properties, PROP_EXA_CONNECTION_STRING);
    }

    public static String getOraConnectionName(final Map<String, String> properties) {
        return getProperty(properties, PROP_ORA_CONNECTION_NAME);
    }

    public static List<String> getTableFilter(final Map<String, String> properties) {
        final String tableNames = getProperty(properties, PROP_TABLES);
        if (!tableNames.isEmpty()) {
            final List<String> tables = Arrays.asList(tableNames.split(","));
            for (int i = 0; i < tables.size(); ++i) {
                tables.set(i, tables.get(i).trim());
            }
            return tables;
        } else {
            return new ArrayList<>();
        }
    }

    public static String getExcludedCapabilities(final Map<String, String> properties) {
        return getProperty(properties, PROP_EXCLUDED_CAPABILITIES);
    }

    public static boolean isLocal(final Map<String, String> properties) {
        return getProperty(properties, PROP_IS_LOCAL).toUpperCase().equals("TRUE");
    }

    public static class ExaConnectionInformationJdbc implements ExaConnectionInformation {
        private final String address;
        private final String user; // can be null
        private final String password; // can be null

        public ExaConnectionInformationJdbc(final String address, final String user, final String password) {
            this.address = address;
            this.user = user;
            this.password = password;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.PASSWORD;
        }

        @Override
        public String getAddress() {
            return this.address;
        }

        @Override
        public String getUser() {
            return this.user;
        }

        @Override
        public String getPassword() {
            return this.password;
        }
    }

    /**
     * Returns the properties as they would be after successfully applying the changes to the existing (old) set of
     * properties.
     *
     * @param oldProperties     properties before the change
     * @param changedProperties changed properties
     * @return properties as they will be applied
     */
    public static Map<String, String> getNewProperties(final Map<String, String> oldProperties,
            final Map<String, String> changedProperties) {
        final Map<String, String> newCompleteProperties = new HashMap<>(oldProperties);
        for (final Map.Entry<String, String> changedProperty : changedProperties.entrySet()) {
            if (changedProperty.getValue() == null) {
                // Null values represent properties which are deleted by the user (might also
                // have never existed actually)
                newCompleteProperties.remove(changedProperty.getKey());
            } else {
                newCompleteProperties.put(changedProperty.getKey(), changedProperty.getValue());
            }
        }
        return newCompleteProperties;
    }

}
