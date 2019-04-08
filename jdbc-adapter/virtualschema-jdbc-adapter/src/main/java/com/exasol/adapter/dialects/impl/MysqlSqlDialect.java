package com.exasol.adapter.dialects.impl;

import java.sql.Connection;
import java.sql.SQLException;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;

/**
 * Dialect for MySQL using the MySQL Connector JDBC driver.
 */
public class MysqlSqlDialect extends AbstractSqlDialect {
    public MysqlSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    private static final String NAME = "MYSQL";

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        return builder.build();
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    @Override
    public String applyQuote(final String identifier) {
        // TODO ANSI_QUOTES option. Must be obtained from JDBC DatabaseMetadata.
        // http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_ansi_quotes
        final CharSequence quoteChar = "`";
        return quoteChar + identifier.replace(quoteChar, quoteChar + "" + quoteChar) + quoteChar;
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        // See
        // http://stackoverflow.com/questions/2051602/mysql-orderby-a-number-nulls-last
        // and also
        // http://stackoverflow.com/questions/9307613/mysql-order-by-null-first-and-desc-after
        assert (this.remoteMetadataReader.getSchemaAdapterNotes().areNullsSortedLow());
        return NullSorting.NULLS_SORTED_LOW;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }
}
