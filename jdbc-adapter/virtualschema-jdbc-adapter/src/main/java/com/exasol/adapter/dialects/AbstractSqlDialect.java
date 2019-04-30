package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
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
    private static final Pattern BOOLEAN_PROPERTY_VALUE_PATTERN = Pattern.compile("^TRUE$|^FALSE$",
            Pattern.CASE_INSENSITIVE);
    protected Set<ScalarFunction> omitParenthesesMap = EnumSet.noneOf(ScalarFunction.class);
    protected RemoteMetadataReader remoteMetadataReader;
    protected AdapterProperties properties;
    protected final Connection connection;

    /**
     * Create a new instance of an {@link AbstractSqlDialect}
     *
     * @param connection JDBC connection to remote data source
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
    public SchemaMetadata readSchemaMetadata() throws SQLException {
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
    public void validateProperties() throws PropertyValidationException {
        validateSupportedPropertiesList();
        validateConnectionProperties();
        validateCatalogNameProperty();
        validateSchemaNameProperty();
        validateDebugOutputAddress();
        validateExceptionHandling();
    }

    protected void validateSupportedPropertiesList() throws PropertyValidationException {
        final List<String> allProperties = new ArrayList<>(this.properties.keySet());
        for (final String property : allProperties) {
            if (!getSupportedProperties().contains(property)) {
                throw new PropertyValidationException(
                        "The dialect " + this.properties.getSqlDialect() + " does not support " + property
                                + " property. Please, do not set the " + property + " property.");
            }
        }
    }

    protected abstract List<String> getSupportedProperties();

    private void validateConnectionProperties() throws PropertyValidationException {
        if (this.properties.containsKey(CONNECTION_NAME_PROPERTY)) {
            if (this.properties.containsKey(CONNECTION_STRING_PROPERTY)
                    || this.properties.containsKey(USERNAME_PROPERTY)
                    || this.properties.containsKey(PASSWORD_PROPERTY)) {
                throw new PropertyValidationException("You specified a connection using the property "
                        + CONNECTION_NAME_PROPERTY + " and therefore should not specify the properties "
                        + CONNECTION_STRING_PROPERTY + ", " + USERNAME_PROPERTY + " and " + PASSWORD_PROPERTY);
            }
        } else {
            if (!this.properties.containsKey(CONNECTION_STRING_PROPERTY)) {
                throw new PropertyValidationException(
                        "You did not specify a connection using the property " + CONNECTION_NAME_PROPERTY
                                + " and therefore have to specify the property " + CONNECTION_STRING_PROPERTY);
            }
        }
    }

    private void validateCatalogNameProperty() throws PropertyValidationException {
        if (this.properties.containsKey(CATALOG_NAME_PROPERTY)
                && (supportsJdbcCatalogs() == StructureElementSupport.NONE)) {
            throw new PropertyValidationException("The dialect " + this.properties.getSqlDialect()
                    + " does not support catalogs. Please, do not set the " + CATALOG_NAME_PROPERTY + " property.");
        }
    }

    private void validateSchemaNameProperty() throws PropertyValidationException {
        if (this.properties.containsKey(SCHEMA_NAME_PROPERTY)
                && (supportsJdbcSchemas() == StructureElementSupport.NONE)) {
            throw new PropertyValidationException("The dialect " + this.properties.getSqlDialect()
                    + " does not support schemas. Please, do not set the " + SCHEMA_NAME_PROPERTY + " property.");
        }
    }

    protected void validateBooleanProperty(final String property) throws PropertyValidationException {
        if (this.properties.containsKey(property) //
                && !BOOLEAN_PROPERTY_VALUE_PATTERN.matcher(this.properties.get(property)).matches()) {
            throw new PropertyValidationException("The value '" + this.properties.get(property) + "' for the property "
                    + property + " is invalid. It has to be either 'true' or 'false' (case insensitive).");
        }
    }

    private void validateDebugOutputAddress() throws PropertyValidationException {
        if (this.properties.containsKey(DEBUG_ADDRESS_PROPERTY)) {
            final String debugAddress = this.properties.getDebugAddress();
            if (!debugAddress.isEmpty()) {
                final String error = "You specified an invalid hostname and port where a log receiver (e.g. `netcat`) "
                        + "is listening for incoming connections. The value of the property " + DEBUG_ADDRESS_PROPERTY
                        + "must adhere to the following format: <host>:<port>, where host is a host name or IP address.";
                if (debugAddress.split(":").length != 2) {
                    throw new PropertyValidationException(error);
                }
                try {
                    Integer.parseInt(debugAddress.split(":")[1]);
                } catch (final NumberFormatException ex) {
                    throw new PropertyValidationException(error);
                }
            }
        }
    }

    private void validateExceptionHandling() throws PropertyValidationException {
        if (this.properties.containsKey(EXCEPTION_HANDLING_PROPERTY)) {
            final String exceptionHandling = this.properties.getExceptionHandling();
            if (!((exceptionHandling == null) || exceptionHandling.isEmpty())) {
                for (final AbstractSqlDialect.ExceptionHandlingMode mode : AbstractSqlDialect.ExceptionHandlingMode
                        .values()) {
                    if (!mode.name().equals(exceptionHandling)) {
                        throw new PropertyValidationException(
                                "Invalid value '" + exceptionHandling + "' for property " + EXCEPTION_HANDLING_PROPERTY
                                        + ". Choose one of: " + ExceptionHandlingMode.IGNORE_INVALID_VIEWS.name() + ", "
                                        + ExceptionHandlingMode.NONE.name());
                    }
                }
            }
        }
    }

    protected void validateDialectName(final String dialectName) throws PropertyValidationException {
        final String availableDialects = "Available dialects: " + SqlDialectRegistry.getInstance().getDialectsString();
        checkIfContainsDialectName(availableDialects);
        checkIfDialectIsSupported(availableDialects);
        checkIfNameIsConsistent(dialectName);
    }

    private void checkIfContainsDialectName(final String availableDialects) throws PropertyValidationException {
        if (!this.properties.containsKey(SQL_DIALECT_PROPERTY)) {
            throw new PropertyValidationException(
                    "You have to specify the SQL dialect (" + SQL_DIALECT_PROPERTY + "). " + availableDialects);
        }
    }

    private void checkIfDialectIsSupported(final String availableDialects) throws PropertyValidationException {
        if (!SqlDialectRegistry.getInstance().isSupported(this.properties.getSqlDialect())) {
            throw new PropertyValidationException(
                    "SQL Dialect \"" + this.properties.getSqlDialect() + "\" is not supported. " + availableDialects);
        }
    }

    private void checkIfNameIsConsistent(final String dialectName) throws PropertyValidationException {
        if (!this.properties.getSqlDialect().equals(dialectName)) {
            throw new PropertyValidationException(
                    "The dialect " + dialectName + " cannot have the name " + this.properties.getSqlDialect()
                            + ". You specified the wrong dialect name or created the wrong dialect class.");
        }
    }

    protected void checkImportPropertyConsistency(final String importFromProperty, final String connectionProperty)
            throws PropertyValidationException {
        final boolean isDirectImport = this.properties.isEnabled(importFromProperty);
        final String value = this.properties.get(connectionProperty);
        final boolean connectionIsEmpty = ((value == null) || value.isEmpty());
        if (isDirectImport) {
            if (connectionIsEmpty) {
                throw new PropertyValidationException("You defined the property " + importFromProperty
                        + ", please also define " + connectionProperty);
            }
        } else {
            if (!connectionIsEmpty) {
                throw new PropertyValidationException("You defined the property " + connectionProperty
                        + " without setting " + importFromProperty + " to 'TRUE'. This is not allowed");
            }
        }
    }

    List<String> getIgnoredErrors() {
        return this.properties.getIgnoredErrors().stream().map(String::toUpperCase).collect(Collectors.toList());
    }
}