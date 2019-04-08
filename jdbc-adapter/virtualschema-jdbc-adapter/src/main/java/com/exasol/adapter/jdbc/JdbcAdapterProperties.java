package com.exasol.adapter.jdbc;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.exasol.*;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialectRegistry;
import com.exasol.adapter.metadata.DataType;

/**
 * Class to expose a nice interface to properties. Casts to the correct data types, checks for valid property values and
 * consistency.
 */
public final class JdbcAdapterProperties {
    static final Logger LOGGER = Logger.getLogger(JdbcAdapterProperties.class.getName());

    // One of the following needs to be set
    static final String PROP_CATALOG_NAME = "CATALOG_NAME";
    static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";
    static final String PROP_CONNECTION_NAME = "CONNECTION_NAME";
    static final String PROP_CONNECTION_STRING = "CONNECTION_STRING";
    static final String PROP_USERNAME = "USERNAME";
    static final String PROP_PASSWORD = "PASSWORD";

    // Optional Parameters
    static final String PROP_TABLES = "TABLE_FILTER";
    static final String PROP_DEBUG_ADDRESS = "DEBUG_ADDRESS";
    static final String PROP_IS_LOCAL = "IS_LOCAL";
    static final String PROP_SQL_DIALECT = "SQL_DIALECT";
    static final String PROP_IMPORT_FROM_EXA = "IMPORT_FROM_EXA";
    static final String PROP_EXA_CONNECTION_STRING = "EXA_CONNECTION_STRING";
    static final String PROP_IMPORT_FROM_ORA = "IMPORT_FROM_ORA";
    static final String PROP_ORA_CONNECTION_NAME = "ORA_CONNECTION_NAME";
    static final String PROP_EXCLUDED_CAPABILITIES = "EXCLUDED_CAPABILITIES";
    static final String PROP_EXCEPTION_HANDLING = "EXCEPTION_HANDLING";
    static final String PROP_LOG_LEVEL = "LOG_LEVEL";
    static final String PROP_IGNORE_ERROR_LIST = "IGNORE_ERRORS";
    static final String PROP_POSTGRESQL_IDENTIFIER_MAPPING = "POSTGRESQL_IDENTIFIER_MAPPING";
    static final String PROP_ORACLE_CAST_NUMBER_TO_DECIMAL = "ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE";

    private static final String DEFAULT_LOG_LEVEL = "INFO";
    private static final String VALUE_SQL_DIALECT_POSTGRESQL = "POSTGRESQL";
    private static final String VALUE_SQL_DIALECT_ORACLE = "ORACLE";
    private static final String VALUE_POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE = "PRESERVE_ORIGINAL_CASE";
    private static final String VALUE_POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER = "CONVERT_TO_UPPER";

    private JdbcAdapterProperties() {
        // prevent instantiation of static helper class
    }

    // Specifies different exception handling strategies
    public enum ExceptionHandlingMode {
        IGNORE_INVALID_VIEWS, NONE
    }

    private static String getProperty(final Map<String, String> properties, final String name,
            final String defaultValue) {
        if (properties.containsKey(name)) {
            return properties.get(name);
        } else {
            return defaultValue;
        }
    }

    private static String getProperty(final Map<String, String> properties, final String name) {
        return getProperty(properties, name, "");
    }

    public static DataType getOracleNumberTargetType(final Map<String, String> properties) {
        final String precisionAndScale = getProperty(properties, PROP_ORACLE_CAST_NUMBER_TO_DECIMAL);
        if (precisionAndScale.trim().isEmpty()) {
            return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        }
        final List<String> precisionAndScaleList = Arrays.stream(precisionAndScale.split(",")).map(String::trim)
                .collect(Collectors.toList());
        return DataType.createDecimal(Integer.valueOf(precisionAndScaleList.get(0)),
                Integer.valueOf(precisionAndScaleList.get(1)));
    }

    public static String getPostgreSQLIdentifierMapping(final Map<String, String> properties) {
        return getProperty(properties, PROP_POSTGRESQL_IDENTIFIER_MAPPING,
                VALUE_POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER);
    }

    public static List<String> getIgnoreErrorList(final Map<String, String> properties) {
        final String ignoreErrors = getProperty(properties, PROP_IGNORE_ERROR_LIST);
        if (ignoreErrors.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(ignoreErrors.split(",")).map(String::trim).map(String::toUpperCase)
                .collect(Collectors.toList());
    }

    public static String getCatalog(final Map<String, String> properties) {
        return getProperty(properties, PROP_CATALOG_NAME);
    }

    public static String getSchema(final Map<String, String> properties) {
        return getProperty(properties, PROP_SCHEMA_NAME);
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

    @Deprecated
    public static void checkPropertyConsistency(final Map<String, String> properties) throws AdapterException {
        validatePropertyValues(properties);
        checkMandatoryProperties(properties);
        checkImportPropertyConsistency(properties, PROP_IMPORT_FROM_EXA, PROP_EXA_CONNECTION_STRING);
        checkImportPropertyConsistency(properties, PROP_IMPORT_FROM_ORA, PROP_ORA_CONNECTION_NAME);
        checkIgnoreErrors(properties);
        checkPostgreSQLIdentifierPropertyConsistency(properties);
        checkOracleSpecificPropertyConsistency(properties);
    }

    private static void checkOracleSpecificPropertyConsistency(final Map<String, String> properties)
            throws InvalidPropertyException {
        if (properties.containsKey(PROP_ORACLE_CAST_NUMBER_TO_DECIMAL)) {
            final String dialectName = getProperty(properties, PROP_SQL_DIALECT).toUpperCase();
            if (!dialectName.equals(VALUE_SQL_DIALECT_ORACLE)) {
                throw new InvalidPropertyException(PROP_ORACLE_CAST_NUMBER_TO_DECIMAL + " can be used only with "
                        + VALUE_SQL_DIALECT_ORACLE + " dialect.");
            }
        }
    }

    private static void checkPostgreSQLIdentifierPropertyConsistency(final Map<String, String> properties)
            throws InvalidPropertyException {
        if (properties.containsKey(PROP_POSTGRESQL_IDENTIFIER_MAPPING)) {
            final String dialectName = getProperty(properties, PROP_SQL_DIALECT);
            if (!dialectName.equals(VALUE_SQL_DIALECT_POSTGRESQL)) {
                throw new InvalidPropertyException(PROP_POSTGRESQL_IDENTIFIER_MAPPING + " can be used only with "
                        + VALUE_SQL_DIALECT_POSTGRESQL + " dialect.");
            }
            final String propertyValue = properties.get(PROP_POSTGRESQL_IDENTIFIER_MAPPING);
            if (!propertyValue.equals(VALUE_POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE)
                    && !propertyValue.equals(VALUE_POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER)) {
                throw new InvalidPropertyException("Value for " + PROP_POSTGRESQL_IDENTIFIER_MAPPING + " must be "
                        + VALUE_POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE + " or "
                        + VALUE_POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER);
            }
        }
    }

    private static void checkIgnoreErrors(final Map<String, String> properties) throws InvalidPropertyException {
        final String dialect = getSqlDialectName(properties);
        final List<String> errorsToIgnore = getIgnoreErrorList(properties);
        for (final String errorToIgnore : errorsToIgnore) {
            if (!errorToIgnore.startsWith(dialect)) {
                throw new InvalidPropertyException(
                        "Error " + errorToIgnore + " cannot be ignored in " + dialect + " dialect.");
            }
        }
    }

    private static void checkImportPropertyConsistency(final Map<String, String> properties,
            final String propImportFromX, final String propConnection) throws InvalidPropertyException {
        final boolean isImport = getProperty(properties, propImportFromX).toUpperCase().equals("TRUE");
        final boolean connectionIsEmpty = getProperty(properties, propConnection).isEmpty();
        if (isImport) {
            if (connectionIsEmpty) {
                throw new InvalidPropertyException(
                        "You defined the property " + propImportFromX + ", please also define " + propConnection);
            }
        } else {
            if (!connectionIsEmpty) {
                throw new InvalidPropertyException("You defined the property " + propConnection + " without setting "
                        + propImportFromX + " to 'TRUE'. This is not allowed");
            }
        }
    }

    private static void validatePropertyValues(final Map<String, String> properties) throws AdapterException {
        validateBooleanProperty(properties, PROP_IS_LOCAL);
        validateBooleanProperty(properties, PROP_IMPORT_FROM_EXA);
        validateBooleanProperty(properties, PROP_IMPORT_FROM_ORA);
        if (properties.containsKey(PROP_DEBUG_ADDRESS)) {
            validateDebugOutputAddress(properties.get(PROP_DEBUG_ADDRESS));
        }
        if (properties.containsKey(PROP_EXCEPTION_HANDLING)) {
            validateExceptionHandling(properties.get(PROP_EXCEPTION_HANDLING));
        }
    }

    private static void validateBooleanProperty(final Map<String, String> properties, final String property)
            throws AdapterException {
        if (properties.containsKey(property)) {
            if (!properties.get(property).toUpperCase().matches("^TRUE$|^FALSE$")) {
                throw new InvalidPropertyException("The value '" + properties.get(property) + "' for the property "
                        + property + " is invalid. It has to be either 'true' or 'false' (case insensitive).");
            }
        }
    }

    private static void validateDebugOutputAddress(final String debugAddress) throws AdapterException {
        if (!debugAddress.isEmpty()) {
            final String error = "You specified an invalid hostname and port for the udf debug service ("
                    + PROP_DEBUG_ADDRESS + "). Please provide a valid value, e.g. 'hostname:3000'";
            if (debugAddress.split(":").length != 2) {
                throw new AdapterException(error);
            }
            try {
                Integer.parseInt(debugAddress.split(":")[1]);
            } catch (final Exception ex) {
                throw new AdapterException(error);
            }
        }
    }

    private static void validateExceptionHandling(final String exceptionHandling) throws AdapterException {
        if (!((exceptionHandling == null) || exceptionHandling.isEmpty())) {
            for (final ExceptionHandlingMode mode : ExceptionHandlingMode.values()) {
                if (mode.name().equals(exceptionHandling)) {
                    return;
                }
            }
            final String error = "You specified an invalid exception mode (" + exceptionHandling + ").";
            throw new AdapterException(error);
        }
    }

    private static void checkMandatoryProperties(final Map<String, String> properties) throws AdapterException {
        final String availableDialects = "Available dialects: " + SqlDialectRegistry.getInstance().getDialectsString();
        if (!properties.containsKey(PROP_SQL_DIALECT)) {
            throw new InvalidPropertyException(
                    "You have to specify the SQL dialect (" + PROP_SQL_DIALECT + "). " + availableDialects);
        }
        if (!SqlDialectRegistry.getInstance().isSupported(properties.get(PROP_SQL_DIALECT))) {
            throw new InvalidPropertyException(
                    "SQL Dialect \"" + properties.get(PROP_SQL_DIALECT) + "\" is not supported. " + availableDialects);
        }
        if (properties.containsKey(PROP_CONNECTION_NAME)) {
            if (properties.containsKey(PROP_CONNECTION_STRING) || properties.containsKey(PROP_USERNAME)
                    || properties.containsKey(PROP_PASSWORD)) {
                throw new InvalidPropertyException("You specified a connection (" + PROP_CONNECTION_NAME
                        + ") and therefore may not specify the properties " + PROP_CONNECTION_STRING + ", "
                        + PROP_USERNAME + " and " + PROP_PASSWORD);
            }
        } else {
            if (!properties.containsKey(PROP_CONNECTION_STRING)) {
                throw new InvalidPropertyException("You did not specify a connection (" + PROP_CONNECTION_NAME
                        + ") and therefore have to specify the property " + PROP_CONNECTION_STRING);
            }
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

    public static String getDebugAddress(final Map<String, String> properties) {
        return getProperty(properties, PROP_DEBUG_ADDRESS);
    }

    public static boolean isLocal(final Map<String, String> properties) {
        return getProperty(properties, PROP_IS_LOCAL).toUpperCase().equals("TRUE");
    }

    public static String getSqlDialectName(final Map<String, String> properties) {
        return getProperty(properties, PROP_SQL_DIALECT);
    }

    public static ExceptionHandlingMode getExceptionHandlingMode(final Map<String, String> properties) {
        final String propertyValue = getProperty(properties, PROP_EXCEPTION_HANDLING);
        if ((propertyValue == null) || propertyValue.isEmpty()) {
            return ExceptionHandlingMode.NONE;
        }
        for (final ExceptionHandlingMode mode : ExceptionHandlingMode.values()) {
            if (mode.name().equals(propertyValue)) {
                return mode;
            }
        }
        return ExceptionHandlingMode.NONE;
    }

    public static Level getLogLevel(final Map<String, String> properties) throws InvalidPropertyException {
        final String levelAsText = getProperty(properties, PROP_LOG_LEVEL, DEFAULT_LOG_LEVEL);
        try {
            return Level.parse(levelAsText);
        } catch (final IllegalArgumentException | NullPointerException e) {
            throw new InvalidPropertyException("Unable to set log level \"" + levelAsText + "\"");
        }
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
