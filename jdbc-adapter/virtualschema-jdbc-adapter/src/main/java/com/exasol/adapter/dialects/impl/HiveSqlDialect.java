package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;

/**
 * Dialect for Hive, using the Cloudera Hive JDBC Driver/Connector (developed by Simba).
 *
 * TODO Finish implementation of this dialect and add as a supported dialect
 */
public class HiveSqlDialect extends AbstractSqlDialect {

    public HiveSqlDialect(SqlDialectContext context) {
        super(context);
    }

    public static final String NAME = "HIVE";

    public String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        Capabilities cap = new Capabilities();
        return cap;
    }

    /**
     * Quote from user manual "The Cloudera JDBC Driver for Apache Hive supports both catalogs and schemas to make it easy for
     * the driver to work with various JDBC applications. Since Hive only organizes tables into
     * schemas/databases, the driver provides a synthetic catalog called “HIVE” under which all of the
     * schemas/databases are organized. The driver also maps the JDBC schema to the Hive
     * schema/database."
     */
    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public String applyQuote(String identifier) {
        // If identifier contains double quotation marks ", it needs to be escaped by another double quotation mark. E.g. "a""b" is the identifier a"b in the db.
        return "`" + identifier + "`";
    }

    @Override
    public String applyQuoteIfNeeded(String identifier) {
        // We need to apply quotes only in case of reserved keywords. Since we don't know these (could look up in JDBC Metadata...) we always quote.
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context) {
        // We need schema qualifiers a) if we are in IS_LOCAL mode, i.e. we run statements directly in a subselect without IMPORT FROM JDBC
        // and b) if we don't have the schema in the jdbc connection string (like "jdbc:exa:localhost:5555;schema=native")
        return true;
        // return context.isLocal();
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SortBy
        // In Hive 2.1.0 and later, specifying the null sorting order for each of
        // the columns in the "order by" clause is supported. The default null sorting
        // order for ASC order is NULLS FIRST, while the default null sorting order for
        // DESC order is NULLS LAST.
        return NullSorting.NULLS_SORTED_LOW;
    }

    @Override
    public String getStringLiteral(String value) {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }

}
