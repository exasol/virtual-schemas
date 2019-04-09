package com.exasol.adapter.dialects;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.jdbc.JdbcAdapterProperties;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Interface for the implementation of a SQL dialect. All data source specific logic is specified here.
 *
 * <p>
 * The responsibilities of a dialect can be be divided into 3 areas:
 * </p>
 *
 * <p>
 * <b>1. Capabilities:</b><br>
 * The dialect defines the set of supported capabilities. See {@link #getCapabilities()} for details.
 * </p>
 *
 * <p>
 * <b>2. Data Type Mapping:</b><br>
 * The dialect defines, how the tables in the data source are mapped to EXASOL virtual tables. In particular the data
 * types have to be mapped to EXASOL data types. See {@link #mapJdbcType(JdbcTypeDescription)} for details.
 * </p>
 *
 * <p>
 * <b>3. SQL Generation:</b><br>
 * The dialect defines how to generate SQL statements in the data source syntax. The dialect provides several methods to
 * customize quoting, case-sensitivity, function name aliases, and other aspects of the syntax.
 *
 * The actual SQL generation is done by the separate class {@link SqlGenerationVisitor} (it uses the visitor pattern).
 * For things like quoting and case-sensitivity, the SQL generation visitor will ask the dialect how to handle them.
 *
 * If your dialect has a special SQL syntax which cannot be realized using the methods of {@link SqlDialect}, then you
 * can implement your own SQL generation visitor which extends {@link SqlGenerationVisitor}. Your custom visitor must
 * then be returned by {@link #getSqlGenerationVisitor(SqlGenerationContext)}. For an example look at
 * {@link com.exasol.adapter.dialects.impl.OracleSqlGenerationVisitor}.
 * </p>
 *
 * <b>Notes for developing a dialect</b>
 *
 * <p>
 * Create a class for your integration test, with the suffix IT.java.
 * </p>
 *
 * <p>
 * We recommend to extend the abstract class {@link AbstractSqlDialect} instead of directly implementing
 * {@link SqlDialect}.
 * </p>
 */
public interface SqlDialect {

    /**
     * @return the name that can be used to choose this dialect (user can give this name). Case insensitive.
     */
    public static String getPublicName() {
        return "SqlDialect interface";
    }

    //
    // CAPABILITIES
    //

    /**
     * @return The set of capabilities supported by this SQL-Dialect
     */
    public Capabilities getCapabilities();

    //
    // MAPPING OF METADATA: CATALOGS, SCHEMAS, TABLES AND DATA TYPES
    //

    public enum SchemaOrCatalogSupport {
        SUPPORTED, UNSUPPORTED, UNKNOWN
    }

    /**
     * @return True, if the database "truly" supports the concept of JDBC catalogs (not just a single dummy catalog). If
     *         true, the user must specify the catalog. False, if the database does not have a catalog concept, e.g. if
     *         it has no catalogs, or a single dummy catalog, or even if it throws an Exception for
     *         {@link DatabaseMetaData#getCatalogs()}. If false, the user must not specify the catalog.
     */
    public SchemaOrCatalogSupport supportsJdbcCatalogs();

    /**
     * @return True, if the database "truly" supports the concept of JDBC schemas (not just a single dummy schema). If
     *         true, the user must specify the schema. False, if the database does not have a schema concept, e.g. if it
     *         has no schemas, or a single dummy schemas, or even if it throws an Exception for
     *         {@link DatabaseMetaData#getSchemas()}. If false, the user must not specify the schema.
     */
    public SchemaOrCatalogSupport supportsJdbcSchemas();

    public class MappedTable {
        private boolean isIgnored = false;
        private String tableName = "";
        private String originalName = "";
        private String tableComment = "";

        public static MappedTable createMappedTable(final String tableName, final String originalName,
                final String tableComment) {
            final MappedTable t = new MappedTable();
            t.isIgnored = false;
            t.tableName = tableName;
            t.originalName = originalName;
            t.tableComment = tableComment;
            return t;
        }

        public static MappedTable createIgnoredTable() {
            final MappedTable t = new MappedTable();
            t.isIgnored = true;
            return t;
        }

        public boolean isIgnored() {
            return this.isIgnored;
        }

        public String getTableName() {
            return this.tableName;
        }

        public String getOriginalTableName() {
            return this.originalName;
        }

        public String getTableComment() {
            return this.tableComment;
        }
    }

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
     * Allows dialect specific handling of different exceptions.
     *
     * @param exception     the caught exception
     * @param exceptionMode exception mode of the adapter
     * @throws SQLException
     * @deprecated this method was never used in any adapters so since 1.9.0 it is deprecated and will be removed.
     */
    @Deprecated
    public void handleException(SQLException exception, JdbcAdapterProperties.ExceptionHandlingMode exceptionMode)
            throws SQLException;

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
     * Read the remote schema metadata from the remote source
     *
     * @param whiteListedRemoteTables selected tables for which the metadata should be read
     * @return remote schema metadata
     */
    public SchemaMetadata readSchemaMetadata(List<String> whiteListedRemoteTables);

    /**
     * Generate a textual description of the columns of a query result (e.g. a push-down query)
     *
     * @param query query for which the column information is retrieved from the remote data source
     * @return column description
     * @throws SQLException if the metadata of the query result cannot be read from the remote data source
     */
    public String describeQueryResultColumns(final String query) throws SQLException;
}