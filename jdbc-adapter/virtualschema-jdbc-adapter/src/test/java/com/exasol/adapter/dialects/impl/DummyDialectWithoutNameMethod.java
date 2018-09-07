package com.exasol.adapter.dialects.impl;

import java.sql.SQLException;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements a dummy of an {@link SqlDialect} without a name method.
 * It is used for tests only.
 */
public class DummyDialectWithoutNameMethod extends AbstractSqlDialect {
    public DummyDialectWithoutNameMethod(final SqlDialectContext context) {
        super(context);
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
    public NullSorting getDefaultNullSorting() {
        return null;
    }

    @Override
    public String getStringLiteral(final String value) {
        return null;
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }
}