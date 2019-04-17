package com.exasol.adapter.dialects;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Abstract implementation of a dialect. We recommend that every dialect should extend this abstract class.
 */
public abstract class AbstractSqlDialect implements SqlDialect {
    // One of the following needs to be set
    static final String SCHEMA_NAME_PROPERTY = "SCHEMA_NAME";
    public static final String CONNECTION_NAME_PROPERTY = "CONNECTION_NAME";
    static final String CONNECTION_STRING_PROPERTY = "CONNECTION_STRING";
    static final String USERNAME_PROPERTY = "USERNAME";
    static final String PASSWORD_PROPERTY = "PASSWORD";

    // Optional Parameters
    static final String DEBUG_ADDRESS_PROPERTY = "DEBUG_ADDRESS";
    static final String IS_LOCAL_PROPERTY = "IS_LOCAL";
    public static final String SQL_DIALECT_PROPERTY = "SQL_DIALECT";

    static final String EXCEPTION_HANDLING_PROPERTY = "EXCEPTION_HANDLING";
    private static final String IGNORE_ERROR_LIST_PROPERTY = "IGNORE_ERRORS";

    protected Set<ScalarFunction> omitParenthesesMap = new HashSet<>();
    protected RemoteMetadataReader remoteMetadataReader;
    protected AdapterProperties properties;
    protected final Connection connection;

    // Specifies different exception handling strategies
    public enum ExceptionHandlingMode {
        IGNORE_INVALID_VIEWS, NONE
    }

    /**
     * Create a new instance of an {@link AbstractSqlDialect}
     *
     * @param properties user properties
     */
    public AbstractSqlDialect(final Connection connection, final AdapterProperties properties) {
        this.connection = connection;
        this.properties = properties;
        this.remoteMetadataReader = createRemoteMetadataReader();
    }

    /**
     * Create the {@link RemoteMetadataReader} that is used to get the database metadata from the remote source.
     * <p>
     * Override this method in the concrete SQL dialect implementation if the dialect requires non-standard metadata
     * mapping.
     *
     * @return metadata reader
     */
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new BaseRemoteMetadataReader(this.connection, this.properties);
    }

    @Override
    public String getTableCatalogAndSchemaSeparator() {
        return ".";
    }

    @Override
    public boolean omitParentheses(final ScalarFunction function) {
        return this.omitParenthesesMap.contains(function);
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new SqlGenerationVisitor(this, context);
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        return new EnumMap<>(ScalarFunction.class);
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aliases = new EnumMap<>(AggregateFunction.class);
        aliases.put(AggregateFunction.GEO_INTERSECTION_AGGREGATE, "ST_INTERSECTION");
        aliases.put(AggregateFunction.GEO_UNION_AGGREGATE, "ST_UNION");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new EnumMap<>(ScalarFunction.class);
        aliases.put(ScalarFunction.ADD, "+");
        aliases.put(ScalarFunction.SUB, "-");
        aliases.put(ScalarFunction.MULT, "*");
        aliases.put(ScalarFunction.FLOAT_DIV, "/");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getPrefixFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new EnumMap<>(ScalarFunction.class);
        aliases.put(ScalarFunction.NEG, "-");
        return aliases;
    }

    @Override
    public String generatePushdownSql(final ConnectionInformation connectionInformation, final String columnDescription,
            final String pushdownSql) {
        final StringBuilder jdbcImportQuery = new StringBuilder();
        if (columnDescription == null) {
            jdbcImportQuery.append("IMPORT FROM JDBC AT ").append(connectionInformation.getCredentials());
        } else {
            jdbcImportQuery.append("IMPORT INTO ").append("(").append(columnDescription).append(")");
            jdbcImportQuery.append(" FROM JDBC AT ").append(connectionInformation.getCredentials());
        }
        jdbcImportQuery.append(" STATEMENT '").append(pushdownSql.replace("'", "''")).append("'");
        return jdbcImportQuery.toString();
    }

    @Override
    public SchemaMetadata readSchemaMetadata() {
        return this.remoteMetadataReader.readRemoteSchemaMetadata();
    }

    @Override
    public SchemaMetadata readSchemaMetadata(final List<String> tables) {
        return this.remoteMetadataReader.readRemoteSchemaMetadata(tables);
    }

    @Override
    public String describeQueryResultColumns(final String query) throws SQLException {
        final ColumnMetadataReader columnMetadataReader = this.remoteMetadataReader.getColumnMetadataReader();
        final ResultSetMetadataReader resultSetMetadataReader = new ResultSetMetadataReader(this.connection,
                columnMetadataReader);
        return resultSetMetadataReader.describeColumns(query);
    }

    @Override
    public void validateProperties(final Map<String, String> properties) throws PropertyValidationException {
        validatePropertyValues(properties);
        checkMandatoryProperties(properties);
    }

    protected void validatePropertyValues(final Map<String, String> properties) throws PropertyValidationException {
        validateBooleanProperty(properties, IS_LOCAL_PROPERTY);
        if (properties.containsKey(DEBUG_ADDRESS_PROPERTY)) {
            validateDebugOutputAddress(properties.get(DEBUG_ADDRESS_PROPERTY));
        }
        if (properties.containsKey(EXCEPTION_HANDLING_PROPERTY)) {
            validateExceptionHandling(properties.get(EXCEPTION_HANDLING_PROPERTY));
        }
    }

    private void checkMandatoryProperties(final Map<String, String> properties) throws PropertyValidationException {
        final String availableDialects = "Available dialects: " + SqlDialectRegistry.getInstance().getDialectsString();
        if (!properties.containsKey(SQL_DIALECT_PROPERTY)) {
            throw new PropertyValidationException(
                    "You have to specify the SQL dialect (" + SQL_DIALECT_PROPERTY + "). " + availableDialects);
        }
        if (!SqlDialectRegistry.getInstance().isSupported(properties.get(SQL_DIALECT_PROPERTY))) {
            throw new PropertyValidationException(
                    "SQL Dialect \"" + properties.get(SQL_DIALECT_PROPERTY) + "\" is not supported. " + availableDialects);
        }
        if (properties.containsKey(CONNECTION_NAME_PROPERTY)) {
            if (properties.containsKey(CONNECTION_STRING_PROPERTY) || properties.containsKey(USERNAME_PROPERTY)
                    || properties.containsKey(PASSWORD_PROPERTY)) {
                throw new PropertyValidationException("You specified a connection (" + CONNECTION_NAME_PROPERTY
                        + ") and therefore may not specify the properties " + CONNECTION_STRING_PROPERTY + ", "
                        + USERNAME_PROPERTY + " and " + PASSWORD_PROPERTY);
            }
        } else {
            if (!properties.containsKey(CONNECTION_STRING_PROPERTY)) {
                throw new PropertyValidationException("You did not specify a connection (" + CONNECTION_NAME_PROPERTY
                        + ") and therefore have to specify the property " + CONNECTION_STRING_PROPERTY);
            }
        }
    }

    protected void validateBooleanProperty(final Map<String, String> properties, final String property)
            throws PropertyValidationException {
        if (properties.containsKey(property) //
                && !properties.get(property).toUpperCase().matches("^TRUE$|^FALSE$")) {
            throw new PropertyValidationException("The value '" + properties.get(property) + "' for the property "
                    + property + " is invalid. It has to be either 'true' or 'false' (case insensitive).");
        }
    }

    private void validateDebugOutputAddress(final String debugAddress) throws PropertyValidationException {
        if (!debugAddress.isEmpty()) {
            final String error = "You specified an invalid hostname and port for the udf debug service ("
                  + DEBUG_ADDRESS_PROPERTY + "). Please provide a valid value, e.g. 'hostname:3000'";
            if (debugAddress.split(":").length != 2) {
                throw new PropertyValidationException(error);
            }
            try {
                Integer.parseInt(debugAddress.split(":")[1]);
            } catch (final Exception ex) {
                throw new PropertyValidationException(error);
            }
        }
    }

    private void validateExceptionHandling(final String exceptionHandling) throws PropertyValidationException {
        if (!((exceptionHandling == null) || exceptionHandling.isEmpty())) {
            for (final AbstractSqlDialect.ExceptionHandlingMode mode : AbstractSqlDialect.ExceptionHandlingMode
                  .values()) {
                if (mode.name().equals(exceptionHandling)) {
                    return;
                }
            }
            final String error = "You specified an invalid exception mode (" + exceptionHandling + ").";
            throw new PropertyValidationException(error);
        }
    }

    protected static String getProperty(final Map<String, String> properties, final String name) {
        return getProperty(properties, name, "");
    }

    protected static String getProperty(final Map<String, String> properties, final String name,
            final String defaultValue) {
        return properties.getOrDefault(name, defaultValue);
    }

    protected void checkIgnoreErrors(final Map<String, String> properties) throws PropertyValidationException {
        final String dialect = getSqlDialectName(properties);
        final List<String> errorsToIgnore = getIgnoreErrorList(properties);
        for (final String errorToIgnore : errorsToIgnore) {
            if (!errorToIgnore.startsWith(dialect)) {
                throw new PropertyValidationException(
                        "Error " + errorToIgnore + " cannot be ignored in " + dialect + " dialect.");
            }
        }
    }

    private String getSqlDialectName(final Map<String, String> properties) {
        return getProperty(properties, SQL_DIALECT_PROPERTY);
    }

    static List<String> getIgnoreErrorList(final Map<String, String> properties) {
        final String ignoreErrors = getProperty(properties, IGNORE_ERROR_LIST_PROPERTY);
        if (ignoreErrors.trim().isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(ignoreErrors.split(",")).map(String::trim).map(String::toUpperCase)
                .collect(Collectors.toList());
    }

    protected void checkImportPropertyConsistency(final Map<String, String> properties, final String propImportFromX,
            final String propConnection) throws PropertyValidationException {
        final boolean isImport = getProperty(properties, propImportFromX).toUpperCase().equals("TRUE");
        final boolean connectionIsEmpty = getProperty(properties, propConnection).isEmpty();
        if (isImport) {
            if (connectionIsEmpty) {
                throw new PropertyValidationException(
                        "You defined the property " + propImportFromX + ", please also define " + propConnection);
            }
        } else {
            if (!connectionIsEmpty) {
                throw new PropertyValidationException("You defined the property " + propConnection + " without setting "
                        + propImportFromX + " to 'TRUE'. This is not allowed");
            }
        }
    }
}