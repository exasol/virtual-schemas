package com.exasol.adapter.dialects.impl;

import java.sql.SQLException;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * This class is work-in-progress
 *
 * TODO The precision of interval type columns is hardcoded, because it cannot
 * be retrieved via JDBC. Should be retrieved from system table.<br>
 * TODO The srid of geometry type columns is hardcoded, because it cannot be
 * retrieved via JDBC. Should be retrieved from system table.<br>
 */
public class ExasolSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "EXASOL";

    public ExasolSqlDialect(final SqlDialectContext context) {
        super(context);
        this.omitParenthesesMap.add(ScalarFunction.SYSDATE);
        this.omitParenthesesMap.add(ScalarFunction.SYSTIMESTAMP);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_SCHEMA);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_SESSION);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_STATEMENT);
        this.omitParenthesesMap.add(ScalarFunction.CURRENT_USER);
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
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();

        switch (jdbcType) {
        case -104:
            // Currently precision is hardcoded, because we cannot retrieve it via EXASOL
            // jdbc driver.
            colType = DataType.createIntervalDaySecond(2, 3);
            break;
        case -103:
            // Currently precision is hardcoded, because we cannot retrieve it via EXASOL
            // jdbc driver.
            colType = DataType.createIntervalYearMonth(2);
            break;
        case 123:
            // Currently srid is hardcoded, because we cannot retrieve it via EXASOL jdbc
            // driver.
            colType = DataType.createGeometry(3857);
            break;
        case 124:
            colType = DataType.createTimestamp(true);
            break;
        }
        return colType;
    }

    @Override
    public Capabilities getCapabilities() {
        // Supports all capabilities
        final Capabilities cap = new Capabilities();
        cap.supportAllCapabilities();
        return cap;
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
        assert (getContext().getSchemaAdapterNotes().isNullsAreSortedHigh());
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(final String value) {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public String generatePushdownSql(final SchemaMetadataInfo meta, ConnectionInformation connectionInformation, String columnDescription, String pushdownSql) {
        ImportType importType = getContext().getImportType();
        if (importType == ImportType.JDBC) {
            return super.generatePushdownSql(meta, connectionInformation, columnDescription, pushdownSql);
        } else if (importType == ImportType.LOCAL) {
            return pushdownSql;
        } else {
            if ((importType != ImportType.EXA)) throw new AssertionError("ExasolSqlDialect has wrong ImportType");
            final StringBuilder exasolImportQuery = new StringBuilder();
            exasolImportQuery.append("IMPORT FROM EXA AT '").append(connectionInformation.getExaConnectionString()).append("' ");
            exasolImportQuery.append(connectionInformation.getCredentials());
            exasolImportQuery.append(" STATEMENT '").append(pushdownSql.replace("'", "''")).append("'");
            return exasolImportQuery.toString();
        }
    }

}
