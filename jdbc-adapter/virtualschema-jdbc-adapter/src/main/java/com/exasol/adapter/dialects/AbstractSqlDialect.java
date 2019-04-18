package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;

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
    protected Set<ScalarFunction> omitParenthesesMap = EnumSet.noneOf(ScalarFunction.class);
    protected RemoteMetadataReader remoteMetadataReader;
    protected AdapterProperties properties;
    protected final Connection connection;

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
    public void validateProperties() throws PropertyValidationException {
        validatePropertyValues();
        checkMandatoryProperties();
    }

    protected void validatePropertyValues() throws PropertyValidationException {
        validateBooleanProperty(IS_LOCAL_PROPERTY);
        if (this.properties.containsKey(DEBUG_ADDRESS_PROPERTY)) {
            validateDebugOutputAddress(this.properties.getDebugAddress());
        }
        if (this.properties.containsKey(EXCEPTION_HANDLING_PROPERTY)) {
            validateExceptionHandling(this.properties.getExceptionHandling());
        }
    }

    private void checkMandatoryProperties() throws PropertyValidationException {
        final String availableDialects = "Available dialects: " + SqlDialectRegistry.getInstance().getDialectsString();
        if (!this.properties.containsKey(SQL_DIALECT_PROPERTY)) {
            throw new PropertyValidationException(
                    "You have to specify the SQL dialect (" + SQL_DIALECT_PROPERTY + "). " + availableDialects);
        }
        if (!SqlDialectRegistry.getInstance().isSupported(this.properties.getSqlDialect())) {
            throw new PropertyValidationException(
                    "SQL Dialect \"" + this.properties.getSqlDialect() + "\" is not supported. " + availableDialects);
        }
        if (this.properties.containsKey(CONNECTION_NAME_PROPERTY)) {
            if (this.properties.containsKey(CONNECTION_STRING_PROPERTY)
                    || this.properties.containsKey(USERNAME_PROPERTY)
                    || this.properties.containsKey(PASSWORD_PROPERTY)) {
                throw new PropertyValidationException("You specified a connection (" + CONNECTION_NAME_PROPERTY
                        + ") and therefore may not specify the properties " + CONNECTION_STRING_PROPERTY + ", "
                        + USERNAME_PROPERTY + " and " + PASSWORD_PROPERTY);
            }
        } else {
            if (!this.properties.containsKey(CONNECTION_STRING_PROPERTY)) {
                throw new PropertyValidationException("You did not specify a connection (" + CONNECTION_NAME_PROPERTY
                        + ") and therefore have to specify the property " + CONNECTION_STRING_PROPERTY);
            }
        }
    }

    protected void validateBooleanProperty(final String property) throws PropertyValidationException {
        if (this.properties.containsKey(property) //
                && !this.properties.get(property).toUpperCase().matches("^TRUE$|^FALSE$")) {
            throw new PropertyValidationException("The value '" + this.properties.get(property) + "' for the property "
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
            } catch (final NumberFormatException ex) {
                throw new PropertyValidationException(error);
            }
        }
    }

    private void validateExceptionHandling(final String exceptionHandling) throws PropertyValidationException {
        if (!((exceptionHandling == null) || exceptionHandling.isEmpty())) {
            for (final AbstractSqlDialect.ExceptionHandlingMode mode : AbstractSqlDialect.ExceptionHandlingMode
                    .values()) {
                if (!mode.name().equals(exceptionHandling)) {
                    throw new PropertyValidationException(
                            "You specified an invalid exception mode (" + exceptionHandling + ").");
                }
            }
        }
    }

    protected String getProperty(final String name) {
        return getProperty(name, "");
    }

    protected String getProperty(final String name, final String defaultValue) {
        if (this.properties.containsKey(name)) {
            return this.properties.get(name);
        } else {
            return defaultValue;
        }
    }

    List<String> getIgnoredErrors() {
        return this.properties.getIgnoredErrors().stream().map(String::toUpperCase).collect(Collectors.toList());
    }

    protected void checkIgnoreErrors() throws PropertyValidationException {
        final String dialect = this.properties.getSqlDialect();
        final List<String> errorsToIgnore = getIgnoredErrors();
        for (final String errorToIgnore : errorsToIgnore) {
            if (!errorToIgnore.startsWith(dialect)) {
                throw new PropertyValidationException(
                        "Error " + errorToIgnore + " cannot be ignored in " + dialect + " dialect.");
            }
        }
    }
}