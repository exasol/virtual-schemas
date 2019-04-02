package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.metadata.DataType;

import java.sql.SQLException;

/**
 * Dialect for MySQL using the MySQL Connector jdbc driver.
 * <p>
 * TODO Finish implementation of this dialect and add as a supported dialect
 */
public class MysqlSqlDialect extends AbstractSqlDialect {
    public MysqlSqlDialect(final SqlDialectContext context) {
        super(context);
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
        assert (getContext().getSchemaAdapterNotes().areNullsSortedLow());
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
