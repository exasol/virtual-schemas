package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;
import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlFunctionAggregate;
import com.exasol.adapter.sql.SqlFunctionScalar;
import com.exasol.adapter.sql.SqlLiteralExactnumeric;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;
import com.exasol.utils.SqlTestUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class SqlDialectTest {

    @Test
    public void testAggregateFunctionAliases() throws AdapterException, MetadataException {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlColumn col1 = new SqlColumn(1, clicksMeta.getColumns().get(0));
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(ImmutableList.<SqlNode>of(
                new SqlFunctionAggregate(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, ImmutableList.<SqlNode>of(col1),
                        false),
                new SqlFunctionAggregate(AggregateFunction.AVG, ImmutableList.<SqlNode>of(col1), false),
                new SqlFunctionAggregate(AggregateFunction.COUNT, new ArrayList<SqlNode>(), true),
                new SqlFunctionAggregate(AggregateFunction.MAX, ImmutableList.<SqlNode>of(col1), false)));
        final SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        final String schemaName = "SCHEMA";
        final String expectedSql = "SELECT NDV(C1), AVERAGE(C1), COUNT2(DISTINCT *), MAX(C1) FROM " + schemaName
                + ".TEST";

        final Map<AggregateFunction, String> aggAliases = new EnumMap<>(AggregateFunction.class);
        final Map<ScalarFunction, String> scalarAliases = ImmutableMap.of();
        final Map<ScalarFunction, String> infixAliases = ImmutableMap.of();
        aggAliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, "NDV");
        aggAliases.put(AggregateFunction.AVG, "AVERAGE");
        aggAliases.put(AggregateFunction.COUNT, "COUNT2");
        final Map<ScalarFunction, String> prefixAliases = ImmutableMap.of();

        final SqlDialect dialect = new AliasesSqlDialect(aggAliases, scalarAliases, infixAliases, prefixAliases);

        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false, false);
        final SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
        final String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    public void testScalarFunctionAliases() throws AdapterException, MetadataException {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlColumn col1 = new SqlColumn(1, clicksMeta.getColumns().get(0));
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(ImmutableList.<SqlNode>of(
                new SqlFunctionScalar(ScalarFunction.ABS, ImmutableList.<SqlNode>of(col1), false, false),
                new SqlFunctionScalar(ScalarFunction.ADD,
                        ImmutableList.of(col1, new SqlLiteralExactnumeric(new BigDecimal(100))), true, false),
                new SqlFunctionScalar(ScalarFunction.SUB,
                        ImmutableList.of(col1, new SqlLiteralExactnumeric(new BigDecimal(100))), true, false),
                new SqlFunctionScalar(ScalarFunction.TO_CHAR, ImmutableList.<SqlNode>of(col1), true, false),
                new SqlFunctionScalar(ScalarFunction.NEG, ImmutableList.<SqlNode>of(col1), false, false)));
        final SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        final String schemaName = "SCHEMA";
        // ADD is infix by default, but must be non-infix after applying the alias.
        final String expectedSql = "SELECT ABSOLUTE(C1), PLUS(C1, 100), (C1 - 100), TO_CHAR(C1), NEGATIVE(C1) FROM "
                + schemaName + ".TEST";

        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ABS, "ABSOLUTE");
        scalarAliases.put(ScalarFunction.ADD, "PLUS");
        scalarAliases.put(ScalarFunction.NEG, "NEGATIVE");
        final SqlDialect dialect = new AliasesSqlDialect(ImmutableMap.<AggregateFunction, String>of(), scalarAliases,
                ImmutableMap.<ScalarFunction, String>of(), ImmutableMap.<ScalarFunction, String>of());

        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false, false);
        final SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
        final String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    public void testInvalidAliases() throws Exception {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        final SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        final SqlGenerationContext context = new SqlGenerationContext("", "schema", false, false);

        // Test non-simple scalar functions
        for (final ScalarFunction function : ScalarFunction.values()) {
            if (!function.isSimple()) {
                final Map<ScalarFunction, String> scalarAliases = ImmutableMap.of(function, "ALIAS");
                final SqlDialect dialect = new AliasesSqlDialect(ImmutableMap.<AggregateFunction, String>of(),
                        scalarAliases, ImmutableMap.<ScalarFunction, String>of(),
                        ImmutableMap.<ScalarFunction, String>of());
                try {
                    final SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
                    throw new Exception("Should never arrive here");
                } catch (final RuntimeException ex) {
                    // This error is expected
                }
            }
        }

        // Test non-simple aggregate functions
        for (final AggregateFunction function : AggregateFunction.values()) {
            if (!function.isSimple()) {
                final Map<AggregateFunction, String> aggregateAliases = ImmutableMap.of(function, "ALIAS");
                final SqlDialect dialect = new AliasesSqlDialect(aggregateAliases,
                        ImmutableMap.<ScalarFunction, String>of(), ImmutableMap.<ScalarFunction, String>of(),
                        ImmutableMap.<ScalarFunction, String>of());
                try {
                    final SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
                    throw new Exception("Should never arrive here");
                } catch (final RuntimeException ex) {
                    // This error is expected
                }
            }
        }
    }

    private TableMetadata getTestTableMetadata() throws MetadataException {
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("C1", "", DataType.createBool(), true, false, "", ""));
        return new TableMetadata("TEST", "", columns, "");
    }

    static class AliasesSqlDialect extends AbstractSqlDialect {

        private final Map<AggregateFunction, String> aggregationAliases;
        private final Map<ScalarFunction, String> scalarAliases;
        private final Map<ScalarFunction, String> infixAliases;
        private final Map<ScalarFunction, String> prefixAliases;

        public AliasesSqlDialect(final Map<AggregateFunction, String> aggregationAliases,
                final Map<ScalarFunction, String> scalarAliases, final Map<ScalarFunction, String> infixAliases,
                final Map<ScalarFunction, String> prefixAliases) {
            super(new SqlDialectContext(new SchemaAdapterNotes(".", "\"", false, false, false, false, false, false,
                    false, false, false, false, true, false)));
            this.aggregationAliases = aggregationAliases;
            this.scalarAliases = scalarAliases;
            this.infixAliases = infixAliases;
            this.prefixAliases = prefixAliases;
        }

        @Override
        public Capabilities getCapabilities() {
            final Capabilities caps = new Capabilities();
            caps.supportAllCapabilities();
            return caps;
        }

        @Override
        public SchemaOrCatalogSupport supportsJdbcCatalogs() {
            return SchemaOrCatalogSupport.UNSUPPORTED;
        }

        @Override
        public SchemaOrCatalogSupport supportsJdbcSchemas() {
            return SchemaOrCatalogSupport.UNSUPPORTED;
        }

        @Override
        public Map<AggregateFunction, String> getAggregateFunctionAliases() {
            return this.aggregationAliases;
        }

        @Override
        public Map<ScalarFunction, String> getScalarFunctionAliases() {
            return this.scalarAliases;
        }

        @Override
        public Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
            if (this.infixAliases.isEmpty()) {
                return super.getBinaryInfixFunctionAliases();
            } else {
                return this.infixAliases;
            }
        }

        @Override
        public Map<ScalarFunction, String> getPrefixFunctionAliases() {
            if (this.prefixAliases.isEmpty()) {
                return super.getPrefixFunctionAliases();
            } else {
                return this.prefixAliases;
            }
        }

        public static String getPublicName() {
            return "TEST";
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
            return "\"" + identifier + "\"";
        }

        @Override
        public String applyQuoteIfNeeded(final String identifier) {
            return identifier; // Intentionally kept simple
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
            return "'" + value + "'";
        }

        @Override
        public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
            return null;
        }
    }

}