package com.exasol.adapter.dialects.exasol;

import static com.exasol.adapter.AdapterProperties.IS_LOCAL_PROPERTY;

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
    public static final String EXASOL_IMPORT_PROPERTY = "IMPORT_FROM_EXA";
    public static final String EXASOL_CONNECTION_STRING_PROPERTY = "EXA_CONNECTION_STRING";

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
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
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
        if (this.properties.isEnabled(IS_LOCAL_PROPERTY)) {
            return ImportType.LOCAL;
        } else if (this.properties.isEnabled(EXASOL_IMPORT_PROPERTY)) {
            return ImportType.EXA;
        } else {
            return ImportType.JDBC;
        }
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName(getPublicName());
        super.validateProperties();
        super.checkImportPropertyConsistency(EXASOL_IMPORT_PROPERTY, EXASOL_CONNECTION_STRING_PROPERTY);
        super.validateBooleanProperty(EXASOL_IMPORT_PROPERTY);
    }
}