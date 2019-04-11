package com.exasol.adapter.dialects.impl;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Exasol SQL dialect
 */
public class ExasolSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "EXASOL";
    static final String LOCAL_IMPORT_PROPERTY = "IS_LOCAL";
    static final String EXASOL_IMPORT_PROPERTY = "IMPORT_FROM_EXA";

    public ExasolSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
        this.omitParenthesesMap.add(ScalarFunction.SYSDATE);
        this.omitParenthesesMap.add(ScalarFunction.SYSTIMESTAMP);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_SCHEMA);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_SESSION);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_STATEMENT);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_USER);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new ExasolMetadataReader(this.connection, this.properties);
    }

    /**
     * Get the name under which the dialect is listed.
     *
     * @return name of the dialect
     */
    public static String getPublicName() {
        return NAME;
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(MainCapability.values());
        builder.addLiteral(LiteralCapability.values());
        builder.addPredicate(PredicateCapability.values());
        builder.addAggregateFunction(AggregateFunctionCapability.values());
        builder.addScalarFunction(ScalarFunctionCapability.values());
        return builder.build();
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
        // If identifier contains double quotation marks ", it needs to be espaced by
        // another double quotation mark. E.g. "a""b" is the identifier a"b in the db.
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        // This is a simplified rule, which quotes all identifiers although not needed
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        // We need schema qualifiers a) if we are in IS_LOCAL mode, i.e. we run
        // statements directly in a subselect without IMPORT FROM JDBC
        // and b) if we don't have the schema in the jdbc connection string (like
        // "jdbc:exa:localhost:5555;schema=native")
        return true;
        // return context.isLocal();
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public String generatePushdownSql(final ConnectionInformation connectionInformation, final String columnDescription,
            final String pushdownSql) {
        if (getImportType() == ImportType.JDBC) {
            return super.generatePushdownSql(connectionInformation, columnDescription, pushdownSql);
        } else if (getImportType() == ImportType.LOCAL) {
            return pushdownSql;
        } else {
            if ((getImportType() != ImportType.EXA)) {
                throw new AssertionError("ExasolSqlDialect has wrong ImportType");
            }
            final StringBuilder exasolImportQuery = new StringBuilder();
            exasolImportQuery.append("IMPORT FROM EXA AT '").append(connectionInformation.getExaConnectionString())
                    .append("' ");
            exasolImportQuery.append(connectionInformation.getCredentials());
            exasolImportQuery.append(" STATEMENT '").append(pushdownSql.replace("'", "''")).append("'");
            return exasolImportQuery.toString();
        }
    }

    /**
     * Return the type of import the Exasol dialect uses
     *
     * @return import type
     */
    public ImportType getImportType() {
        if (this.properties.isEnabled(LOCAL_IMPORT_PROPERTY)) {
            return ImportType.LOCAL;
        } else if (this.properties.isEnabled(EXASOL_IMPORT_PROPERTY)) {
            return ImportType.EXA;
        } else {
            return ImportType.JDBC;
        }
    }
}