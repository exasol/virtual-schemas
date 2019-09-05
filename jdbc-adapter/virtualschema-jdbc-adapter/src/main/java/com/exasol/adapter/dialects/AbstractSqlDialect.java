package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.sql.*;

/**
 * Abstract implementation of a dialect. We recommend that every dialect should extend this abstract class.
 */
public abstract class AbstractSqlDialect implements SqlDialect {
    protected Set<ScalarFunction> omitParenthesesMap = EnumSet.noneOf(ScalarFunction.class);
    protected RemoteMetadataReader remoteMetadataReader;
    protected AdapterProperties properties;
    protected final Connection connection;
    protected QueryRewriter queryRewriter;
    private static final Pattern BOOLEAN_PROPERTY_VALUE_PATTERN = Pattern.compile("^TRUE$|^FALSE$",
            Pattern.CASE_INSENSITIVE);
    private static final Logger LOGGER = Logger.getLogger(AbstractSqlDialect.class.getName());

    /**
     * Create a new instance of an {@link AbstractSqlDialect}.
     *
     * @param connection JDBC connection to remote data source
     * @param properties user properties
     */
    public AbstractSqlDialect(final Connection connection, final AdapterProperties properties) {
        this.connection = connection;
        this.properties = properties;
        this.remoteMetadataReader = createRemoteMetadataReader();
        this.queryRewriter = createQueryRewriter();
    }

    /**
     * Create the {@link RemoteMetadataReader} that is used to get the database metadata from the remote source.
     * <p>
     * Override this method in the concrete SQL dialect implementation to choose the right metadata reader.
     *
     * @return metadata reader
     */
    protected abstract RemoteMetadataReader createRemoteMetadataReader();

    /**
     * Create the {@link QueryRewriter} that is used to create the final SQL query sent back from the Virtual Schema
     * backend to the Virtual Schema frontend in a push-down scenario.
     * <p>
     * Override this method in the concrete SQL dialect implementation to choose the right query rewriter.
     *
     * @return query rewriter
     */
    protected abstract QueryRewriter createQueryRewriter();

    @Override
    public String getTableCatalogAndSchemaSeparator() {
        return ".";
    }

    @Override
    public boolean omitParentheses(final ScalarFunction function) {
        return this.omitParenthesesMap.contains(function);
    }

    @Override
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
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
    public String rewriteQuery(final SqlStatement statement, final ExaMetadata exaMetadata)
            throws AdapterException, SQLException {
        return this.queryRewriter.rewrite(statement, exaMetadata, this.properties);
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
    public String getStringLiteral(final String value) {
        if (value == null) {
            return "NULL";
        } else {
            return "'" + value.replace("'", "''") + "'";
        }
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
                final String unsupportedElement = property;
                throw new PropertyValidationException(createUnsupportedElementMessage(unsupportedElement, property));
            }
        }
    }

    /**
     * Get the list of user-defined adapter properties which the dialect supports.
     *
     * @return list of supported properties
     */
    protected abstract List<String> getSupportedProperties();

    protected String createUnsupportedElementMessage(final String unsupportedElement, final String property) {
        return "The dialect " + this.properties.getSqlDialect() + " does not support " + unsupportedElement
                + " property. Please, do not set the " + property + " property.";
    }

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
            throw new PropertyValidationException(createUnsupportedElementMessage("catalogs", CATALOG_NAME_PROPERTY));
        }
    }

    private void validateSchemaNameProperty() throws PropertyValidationException {
        if (this.properties.containsKey(SCHEMA_NAME_PROPERTY)
                && (supportsJdbcSchemas() == StructureElementSupport.NONE)) {
            throw new PropertyValidationException(createUnsupportedElementMessage("schemas", SCHEMA_NAME_PROPERTY));
        }
    }

    protected void validateBooleanProperty(final String property) throws PropertyValidationException {
        if (this.properties.containsKey(property) //
                && !BOOLEAN_PROPERTY_VALUE_PATTERN.matcher(this.properties.get(property)).matches()) {
            throw new PropertyValidationException("The value '" + this.properties.get(property) + "' for the property "
                    + property + " is invalid. It has to be either 'true' or 'false' (case insensitive).");
        }
    }

    private void validateDebugOutputAddress() {
        if (this.properties.containsKey(DEBUG_ADDRESS_PROPERTY)) {
            final String debugAddress = this.properties.getDebugAddress();
            if (!debugAddress.isEmpty()) {
                validateDebugPortNumber(debugAddress);
            }
        }
    }

    // Note that this method intentionally does not throw a validation exception but rather creates log warnings. This
    // allows dropping a schema even if the debug output port is misconfigured. Logging falls back to local logging in
    // this case.
    private void validateDebugPortNumber(final String debugAddress) {
        final int colonLocation = debugAddress.lastIndexOf(':');
        if (colonLocation > 0) {
            final String portAsString = debugAddress.substring(colonLocation + 1);
            try {
                final int port = Integer.parseInt(portAsString);
                if ((port < 1) || (port > 65535)) {
                    LOGGER.warning(() -> "Debug output port " + port + " is out of range. Port specified in property "
                            + DEBUG_ADDRESS_PROPERTY
                            + "must have following format: <host>[:<port>], and be between 1 and 65535.");
                }
            } catch (final NumberFormatException ex) {
                LOGGER.warning(() -> "Illegal debug output port \"" + portAsString + "\". Property "
                        + DEBUG_ADDRESS_PROPERTY
                        + "must have following format: <host>[:<port>], where port is a number between 1 and 65535.");
            }
        }
    }

    private void validateExceptionHandling() throws PropertyValidationException {
        if (this.properties.containsKey(EXCEPTION_HANDLING_PROPERTY)) {
            final String exceptionHandling = this.properties.getExceptionHandling();
            if (!((exceptionHandling == null) || exceptionHandling.isEmpty())) {
                for (final SqlDialect.ExceptionHandlingMode mode : SqlDialect.ExceptionHandlingMode.values()) {
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

    protected void validateCastNumberToDecimalProperty(final String castNumberToDecimalProperty)
            throws PropertyValidationException {
        if (this.properties.containsKey(castNumberToDecimalProperty)) {
            final Pattern pattern = Pattern.compile("\\s*(\\d+)\\s*,\\s*(\\d+)\\s*");
            final String precisionAndScale = this.properties.get(castNumberToDecimalProperty);
            final Matcher matcher = pattern.matcher(precisionAndScale);
            if (!matcher.matches()) {
                throw new PropertyValidationException("Unable to parse adapter property " + castNumberToDecimalProperty
                        + " value \"" + precisionAndScale
                        + " into a number's precision and scale. The required format is \"<precision>.<scale>\", where "
                        + "both are integer numbers.");
            }
        }
    }
}
