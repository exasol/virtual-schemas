package com.exasol.adapter.dialects;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.sql.*;

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
     * Get the dialect name.
     *
     * @return name of the dialect
     */
    public String getName();

    /**
     * Get the capabilities the SQL dialect supports.
     *
     * @return set of capabilities supported by this SQL-Dialect
     */
    public Capabilities getCapabilities();

    /**
     * Multiplicity that the remote data source supports for a structural element like catalogs or a schemas.
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
     * Dialects that support a single database should not use <code>AUTO_DETECT</code> because this unnecessarily costs
     * performance.
     */
    public enum StructureElementSupport {
        NONE, SINGLE, MULTIPLE, AUTO_DETECT
    }

    /**
     * This enumeration specifies different exception handling strategies.
     */
    public enum ExceptionHandlingMode {
        IGNORE_INVALID_VIEWS, NONE
    }

    /**
     * Define whether the remote source supports catalogs and if it does, check if it supports a single "pseudo" catalog
     * or multiple catalogs.
     *
     * @return <code>NONE</code> if the dialect does not support catalogs at all, <code>SINGLE</code> if it supports
     *         exactly one (like e.g. Exasol) or <code>MULTIPLE</code> if real catalogs are supported.
     */
    public StructureElementSupport supportsJdbcCatalogs();

    /**
     * Define whether the remote source supports schemas and if it does, check if it supports a single "pseudo" schema
     * or multiple schemas.
     *
     * @return <code>NONE</code> if the dialect does not support schemas at all, <code>SINGLE</code> if it supports
     *         exactly one (like e.g. Exasol) or <code>MULTIPLE</code> if real schemas are supported.
     */
    public StructureElementSupport supportsJdbcSchemas();

    /**
     * @param identifier The name of an identifier (table or column). If identifiers are case sensitive, the identifier
     *                   must be passed case-sensitive of course.
     * @return the quoted identifier, also if quoting is not required
     */
    public String applyQuote(String identifier);

    /**
     * Check whether the dialect expects table identifiers to be qualified with the catalog.
     *
     * <p>
     * Example:
     * <p>
     * <code>SELECT * FROM MY_CATALOG.MY_TABLE</code>
     * <p>
     * Can be combined with {@link #requiresSchemaQualifiedTableNames(SqlGenerationContext)}.
     *
     * @param context context for SQL generation
     *
     * @return <code>true</code> if table names must be catalog-qualified
     */
    public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context);

    /**
     * Check whether the dialect expects table identifiers to be qualified with the schema.
     *
     * <p>
     * Example:
     * <p>
     * <code>SELECT * FROM MY_SCHEMA.MY_TABLE</code>
     * <p>
     * Can be combined with {@link #requiresCatalogQualifiedTableNames(SqlGenerationContext)}.
     *
     * @param context context for SQL generation
     *
     * @return <code>true</code> if table names must be schema-qualified
     */
    public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context);

    /**
     * @return String that is used to separate the catalog and/or the schema from the table name. In many cases this is
     *         a dot.
     */
    public String getTableCatalogAndSchemaSeparator();

    /**
     * Definition of where <code>NULL</code> values appear in a search result.
     *
     * <dl>
     * <dt><code>NULLS_SORTED_AT_END</code></dt>
     * <dd>Independently of the actual search order <code>NULL</code> values always appear at the <em>end</em> of the
     * result set</dd>
     * <dt><code>NULLS_SORTED_AT_START</code></dt>
     * <dd>Independently of the actual search order <code>NULL</code> values always appear at the <em>start</em> of the
     * result set
     * <dt><code>NULLS_SORTED_HIGH</code></dt>
     * <dd><code>NULL</code> values appear at the start of the result set when sorted in descending order, and at the
     * end when sorted ascending</dd>
     * <dt><code>NULLS_SORTED_LOW</code></dt>
     * <dd><code>NULL</code>values appear at the end of the result set when sorted in descending order, and at the start
     * when sorted ascending</dd>
     * </dl>
     */
    public enum NullSorting {
        NULLS_SORTED_AT_END, NULLS_SORTED_AT_START, NULLS_SORTED_HIGH, NULLS_SORTED_LOW
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
     * @return the string literal in valid SQL syntax, e.g. "value" becomes "'value'". This might include escaping.
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
     * Check whether a parentheses should be omitted for a function.
     *
     * @param function function for which the necessity of parentheses is evaluated
     *
     * @return Returns true for functions with zero arguments if they do not require parentheses (e.g. SYSDATE).
     */
    public boolean omitParentheses(ScalarFunction function);

    /**
     * Returns the Visitor to be used for SQL generation. Use this only if you need to, i.e. if you have requirements
     * which cannot be realized via the other methods provided by {@link SqlDialect}.
     *
     * @param context context information for the SQL generation visitor
     * @return the SqlGenerationVisitor to be used for this dialect
     */
    public SqlNodeVisitor<String> getSqlGenerationVisitor(SqlGenerationContext context);

    /**
     * Read the remote schema metadata for all tables from the remote source.
     *
     * @return remote schema metadata
     * @throws SQLException if reading the remote metadata fails
     */
    public SchemaMetadata readSchemaMetadata() throws SQLException;

    /**
     * Read the remote schema metadata for selected from the remote source.
     *
     * @param tables selected tables for which the metadata should be read
     * @return remote schema metadata
     */
    public SchemaMetadata readSchemaMetadata(List<String> tables);

    /**
     * Validate user-defined properties and throw exception if they are incorrect.
     *
     * @throws PropertyValidationException if some properties are used incorrectly
     */
    public void validateProperties() throws PropertyValidationException;

    /**
     * Rewrite the given query so that data from the remote data source is imported into Exasol when that query is
     * executed on the Virtual Schema frontend.
     *
     * @param statement   original query as sent by the Virtual Schema frontend
     * @param exaMetadata Exasol metadata
     * @return rewritten query
     * @throws SQLException     if execution of any SQL command on the remote data source failed
     * @throws AdapterException if rewriting the query failed
     */
    public String rewriteQuery(SqlStatement statement, ExaMetadata exaMetadata) throws AdapterException, SQLException;
}
