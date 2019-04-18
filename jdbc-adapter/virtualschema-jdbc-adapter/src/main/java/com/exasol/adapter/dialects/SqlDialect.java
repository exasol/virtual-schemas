package com.exasol.adapter.dialects;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.jdbc.PropertyValidationException;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Interface for the implementation of a SQL dialect.
 * <p>
 * Dialects define the capabilities and the behavior of the adapter implementation for a specific remote database.
 * <p>
 * For information about how the metadata of the remote data source is mapped, please refer to the list of interfaces
 * below.
 *
 * @see com.exasol.adapter.jdbc.RemoteMetadataReader
 * @see com.exasol.adapter.jdbc.TableMetadataReader
 * @see com.exasol.adapter.jdbc.ColumnMetadataReader
 */
public interface SqlDialect {
    /**
     * @return the name that can be used to choose this dialect (user can give this name). Case insensitive.
     */
    public static String getPublicName() {
        return "SqlDialect interface";
    }

    /**
     * @return The set of capabilities supported by this SQL-Dialect
     */
    public Capabilities getCapabilities();

    /**
     * Multiplicity that the remote data source supports for a structural element like catalogs or a schemas.
     * <p>
     * <dl>
     * <dt><code>NONE</code></dt>
     * <dd>database does not support the structural element</dd>
     * <dt><code>SINGLE</code></dt>
     * <dd>database uses a single (pseudo) structural element</dd>
     * <dt><code>MULTIPLE</code></dt>
     * <dd>database truly supports this element with multiple possible entries</dd>
     * <dt><code>AUTO_DETECT</code></dt>
     * <dd>dialect auto-detects support for this element</dd>
     * </dl>
     * </p>
     * Dialects that support a single database should not use <code>AUTO_DETECT</code> because this unnecessarily costs
     * performance.
     */
    public enum StructureElementSupport {
        NONE, SINGLE, MULTIPLE, AUTO_DETECT
    }

    // Specifies different exception handling strategies
    public enum ExceptionHandlingMode {
        IGNORE_INVALID_VIEWS, NONE
    }

    /**
     * Define whether the remote source supports catalogs and if it does, whether is supports a single "pseudo" catalog
     * or multiple catalogs.
     *
     * @return <code>NONE</code> if the dialect does not support catalogs at all, <code>SINGLE</code> if it supports
     *         exactly one (like e.g. Exasol) or <code>MULTIPLE</code> if real catalogs are supported.
     */
    public StructureElementSupport supportsJdbcCatalogs();

    /**
     * Define whether the remote source supports schemas and if it does, whether is supports a single "pseudo" schema or
     * multiple schemas.
     *
     * @return <code>NONE</code> if the dialect does not support schemas at all, <code>SINGLE</code> if it supports
     *         exactly one (like e.g. Exasol) or <code>MULTIPLE</code> if real schemas are supported.
     */
    public StructureElementSupport supportsJdbcSchemas();

    /**
     * How unquoted or quoted identifiers in queries or DDLs are handled
     */
    public enum IdentifierCaseHandling {
        INTERPRET_AS_LOWER, INTERPRET_AS_UPPER, INTERPRET_CASE_SENSITIVE
    }

    /**
     * @return How to handle case sensitivity of unquoted identifiers
     */
    public IdentifierCaseHandling getUnquotedIdentifierHandling();

    /**
     * @return How to handle case sensitivity of quoted identifiers
     */
    public IdentifierCaseHandling getQuotedIdentifierHandling();

    /**
     * @param identifier The name of an identifier (table or column). If identifiers are case sensitive, the identifier
     *                   must be passed case-sensitive of course.
     * @return the quoted identifier, also if quoting is not required
     */
    public String applyQuote(String identifier);

    /**
     * @param identifier The name of an identifier (table or column).
     * @return the quoted identifier, if this name requires quoting, or the unquoted identifier, if no quoting is
     *         required.
     */
    public String applyQuoteIfNeeded(String identifier);

    /**
     * @return True if table names must be catalog-qualified, e.g. SELECT * FROM MY_CATALOG.MY_TABLE, otherwise false.
     *         Can be combined with {@link #requiresSchemaQualifiedTableNames(SqlGenerationContext)}
     */
    public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context);

    /**
     * @return True if table names must be schema-qualified, e.g. SELECT * FROM MY_SCHEMA.MY_TABLE, otherwise false. Can
     *         be combined with {@link #requiresCatalogQualifiedTableNames(SqlGenerationContext)}
     */
    public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context);

    /**
     * @return String that is used to separate the catalog and/or the schema from the tablename. In many cases this is a
     *         dot.
     */
    public String getTableCatalogAndSchemaSeparator();

    public enum NullSorting {
        // NULL values are sorted at the end regardless of sort order
        NULLS_SORTED_AT_END,

        // NULL values are sorted at the start regardless of sort order
        NULLS_SORTED_AT_START,

        // NULL values are sorted high
        NULLS_SORTED_HIGH,

        // NULL values are sorted low
        NULLS_SORTED_LOW
    }

    /**
     * @return The behavior how nulls are sorted in an ORDER BY. If the null sorting behavior is not
     *         {@link NullSorting#NULLS_SORTED_AT_END} and your dialects has the order by capability but you cannot
     *         explicitly specify NULLS FIRST or NULLS LAST, then you must overwrite the SQL generation to somehow
     *         obtain the desired semantic.
     */
    public NullSorting getDefaultNullSorting();

    /**
     * @param value a string literal value
     * @return the string literal in valid SQL syntax, e.g. "value" becomes "'value'". This might include escaping
     */
    public String getStringLiteral(String value);

    /**
     * @return aliases for scalar functions. To be defined for each function that has the same semantic but a different
     *         name in the data source. If an alias for the same function is defined in
     *         {@link #getBinaryInfixFunctionAliases()}, than the infix alias will be ignored.
     */
    public Map<ScalarFunction, String> getScalarFunctionAliases();

    /**
     * @return Defines which binary scalar functions should be treated infix and how. E.g. a map entry ("ADD", "+")
     *         causes a function call "ADD(1,2)" to be written as "1 + 2".
     */
    public Map<ScalarFunction, String> getBinaryInfixFunctionAliases();

    /**
     * @return Defines which unary scalar functions should be treated prefix and how. E.g. a map entry ("NEG", "-")
     *         causes a function call "NEG(2)" to be written as "-2".
     */
    public Map<ScalarFunction, String> getPrefixFunctionAliases();

    /**
     * @return aliases for aggregate functions. To be defined for each function that has the same semantic but a
     *         different name in the data source.
     */
    public Map<AggregateFunction, String> getAggregateFunctionAliases();

    /**
     * @return Returns true for functions with zero arguments if they do not require parentheses (e.g. SYSDATE).
     */
    public boolean omitParentheses(ScalarFunction function);

    /**
     * Returns the Visitor to be used for SQL generation. Use this only if you need to, i.e. if you have requirements
     * which cannot be realized via the other methods provided by {@link SqlDialect}.
     *
     * @param context context information for the sql generation visitor
     * @return the SqlGenerationVisitor to be used for this dialect
     */
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context);

    /**
     * Returns the final pushdown SQL statement. This means generally encapsulating the given SQL query in an IMPORT
     * statement.
     *
     * @param connectionInformation contains all values concerning the connection that are needed to build the IMPORT
     *                              statement
     * @param columnDescription     column names and types to be added to the IMPORT statement
     * @param pushdownSql           the SQL text generated by the SqlGenerationVisitor {@link SqlGenerationVisitor}
     * @return SQL string that is the pushdown SQL sent to the EXASOL database
     */
    public String generatePushdownSql(ConnectionInformation connectionInformation, String columnDescription,
            String pushdownSql);

    /**
     * Read the remote schema metadata for all tables from the remote source
     *
     * @return remote schema metadata
     */
    public SchemaMetadata readSchemaMetadata();

    /**
     * Read the remote schema metadata for selected from the remote source
     *
     * @param tables selected tables for which the metadata should be read
     * @return remote schema metadata
     */
    public SchemaMetadata readSchemaMetadata(List<String> tables);

    /**
     * Generate a textual description of the columns of a query result (e.g. a push-down query)
     *
     * @param query query for which the column information is retrieved from the remote data source
     * @return column description
     * @throws SQLException if the metadata of the query result cannot be read from the remote data source
     */
    public String describeQueryResultColumns(final String query) throws SQLException;

    public void validateProperties() throws PropertyValidationException;
}