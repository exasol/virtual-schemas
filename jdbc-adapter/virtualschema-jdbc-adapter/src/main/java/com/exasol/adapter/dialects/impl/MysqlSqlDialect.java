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
 *
 * TODO Finish implementation of this dialect and add as a supported dialect
 */
public class MysqlSqlDialect extends AbstractSqlDialect {

    public MysqlSqlDialect(SqlDialectContext context) {
        super(context);
    }

    public static final String NAME = "MYSQL";

    public String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        Capabilities cap = new Capabilities();
        return cap;
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
    public String applyQuote(String identifier) {
        // TODO ANSI_QUOTES option. Must be obtained from JDBC DatabaseMetadata. http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_ansi_quotes
        CharSequence quoteChar = "`";
        return quoteChar + identifier.replace(quoteChar, quoteChar + "" + quoteChar) + quoteChar;
    }

    @Override
    public String applyQuoteIfNeeded(String identifier) {
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context) {
        return false;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        // See http://stackoverflow.com/questions/2051602/mysql-orderby-a-number-nulls-last
        // and also http://stackoverflow.com/questions/9307613/mysql-order-by-null-first-and-desc-after
        assert(getContext().getSchemaAdapterNotes().isNullsAreSortedLow());
        return NullSorting.NULLS_SORTED_LOW;
    }

    @Override
    public String getStringLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public DataType dialectSpecificMapJdbcType(JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }
}
