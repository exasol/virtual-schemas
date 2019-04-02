package com.exasol.adapter.dialects;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.jdbc.JdbcAdapterProperties;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;
import com.sun.xml.internal.ws.api.databinding.MetadataReader;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DummySqlDialect implements SqlDialect {

    public static String getPublicName() {
        return "dummy dialect";
    }

    public Map<String, String> getProperties() {
        return null;
    }

    public MetadataReader getMetadataReader() {
        return null;
    }

    @Override
    public Capabilities getCapabilities() {
        return null;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return null;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return null;
    }

    @Override
    public MappedTable mapTable(final ResultSet tables, final List<String> ignoreErrorList) throws SQLException {
        return null;
    }

    @Override
    public ColumnMetadata mapColumn(final ResultSet columns) throws SQLException {
        return null;
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return null;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return null;
    }

    @Override
    public String applyQuote(final String identifier) {
        return null;
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        return null;
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public String getTableCatalogAndSchemaSeparator() {
        return null;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return null;
    }

    @Override
    public String getStringLiteral(final String value) {
        return null;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        return null;
    }

    @Override
    public Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
        return null;
    }

    @Override
    public Map<ScalarFunction, String> getPrefixFunctionAliases() {
        return null;
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        return null;
    }

    @Override
    public boolean omitParentheses(final ScalarFunction function) {
        return false;
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return null;
    }

    @Override
    public void handleException(final SQLException exception,
          final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode) throws SQLException {

    }

    @Override
    public String generatePushdownSql(final ConnectionInformation connectionInformation, final String columnDescription,
          final String pushdownSql) {
        return null;
    }
}
