package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.*;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import com.exasol.sql.SqlNormalizer;

class SqlDialectTest {
    @Test
    void testAggregateFunctionAliases() throws AdapterException {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlColumn col1 = new SqlColumn(1, clicksMeta.getColumns().get(0));
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(
                List.of(new SqlFunctionAggregate(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, List.of(col1), false),
                        new SqlFunctionAggregate(AggregateFunction.AVG, List.of(col1), false),
                        new SqlFunctionAggregate(AggregateFunction.COUNT, new ArrayList<SqlNode>(), true),
                        new SqlFunctionAggregate(AggregateFunction.MAX, List.of(col1), false)));
        final SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        final String schemaName = "SCHEMA";
        final String expectedSql = "SELECT NDV(\"C1\"), AVERAGE(\"C1\"), COUNT2(DISTINCT *), MAX(\"C1\") FROM \""
                + schemaName + "\".\"TEST\"";

        final Map<AggregateFunction, String> aggAliases = new EnumMap<>(AggregateFunction.class);
        final Map<ScalarFunction, String> scalarAliases = Map.of();
        final Map<ScalarFunction, String> infixAliases = Map.of();
        aggAliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, "NDV");
        aggAliases.put(AggregateFunction.AVG, "AVERAGE");
        aggAliases.put(AggregateFunction.COUNT, "COUNT2");
        final Map<ScalarFunction, String> prefixAliases = Map.of();

        final SqlDialect dialect = new AliasesSqlDialect(aggAliases, scalarAliases, infixAliases, prefixAliases);

        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        final SqlNodeVisitor<String> generator = new SqlGenerationVisitor(dialect, context);
        final String actualSql = node.accept(generator);
        assertEquals(SqlNormalizer.normalizeSql(expectedSql), SqlNormalizer.normalizeSql(actualSql));
    }

    @Test
    void testScalarFunctionAliases() throws AdapterException {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlColumn col1 = new SqlColumn(1, clicksMeta.getColumns().get(0));
        final SqlSelectList selectList = SqlSelectList
                .createRegularSelectList(List.of(new SqlFunctionScalar(ScalarFunction.ABS, List.of(col1), false, false),
                        new SqlFunctionScalar(ScalarFunction.ADD,
                                List.of(col1, new SqlLiteralExactnumeric(new BigDecimal(100))), true, false),
                        new SqlFunctionScalar(ScalarFunction.SUB,
                                List.of(col1, new SqlLiteralExactnumeric(new BigDecimal(100))), true, false),
                        new SqlFunctionScalar(ScalarFunction.TO_CHAR, List.of(col1), true, false),
                        new SqlFunctionScalar(ScalarFunction.NEG, List.of(col1), false, false)));
        final SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        final String schemaName = "SCHEMA";
        // ADD is infix by default, but must be non-infix after applying the alias.
        final String expectedSql = "SELECT ABSOLUTE(\"C1\"), PLUS(\"C1\", 100), (\"C1\" - 100), TO_CHAR(\"C1\"), NEGATIVE(\"C1\") FROM \""
                + schemaName + "\".\"TEST\"";

        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ABS, "ABSOLUTE");
        scalarAliases.put(ScalarFunction.ADD, "PLUS");
        scalarAliases.put(ScalarFunction.NEG, "NEGATIVE");
        final SqlDialect dialect = new AliasesSqlDialect(Map.of(), scalarAliases, Map.of(), Map.of());

        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        final SqlNodeVisitor<String> generator = new SqlGenerationVisitor(dialect, context);
        final String actualSql = node.accept(generator);
        assertEquals(SqlNormalizer.normalizeSql(expectedSql), SqlNormalizer.normalizeSql(actualSql));
    }

    @Test
    void testInvalidAliases() throws Exception {
        final SqlGenerationContext context = new SqlGenerationContext("", "schema", false);
        for (final ScalarFunction function : ScalarFunction.values()) {
            if (!function.isSimple()) {
                final Map<ScalarFunction, String> scalarAliases = Map.of(function, "ALIAS");
                final SqlDialect dialect = new AliasesSqlDialect(Map.of(), scalarAliases, Map.of(), Map.of());
                try {
                    new SqlGenerationVisitor(dialect, context);
                    throw new Exception("Should never arrive here");
                } catch (final RuntimeException ex) {
                    // This error is expected
                }
            }
        }

        // Test non-simple aggregate functions
        for (final AggregateFunction function : AggregateFunction.values()) {
            if (!function.isSimple()) {
                final Map<AggregateFunction, String> aggregateAliases = Map.of(function, "ALIAS");
                final SqlDialect dialect = new AliasesSqlDialect(aggregateAliases, Map.of(), Map.of(), Map.of());
                try {
                    new SqlGenerationVisitor(dialect, context);
                    throw new Exception("Should never arrive here");
                } catch (final RuntimeException ex) {
                    // This error is expected
                }
            }
        }
    }

    private TableMetadata getTestTableMetadata() {
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("C1").adapterNotes("").type(DataType.createBool()).nullable(true)
                .identity(false).defaultValue("").comment("").build());
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
            super(null, AdapterProperties.emptyProperties());

            this.aggregationAliases = aggregationAliases;
            this.scalarAliases = scalarAliases;
            this.infixAliases = infixAliases;
            this.prefixAliases = prefixAliases;
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
        public StructureElementSupport supportsJdbcCatalogs() {
            return StructureElementSupport.NONE;
        }

        @Override
        public StructureElementSupport supportsJdbcSchemas() {
            return StructureElementSupport.NONE;
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

        @Override
        protected List<String> getSupportedProperties() {
            return null;
        }

        @Override
        public String getName() {
            return "TEST";
        }

        @Override
        public String applyQuote(final String identifier) {
            return "\"" + identifier + "\"";
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
        protected RemoteMetadataReader createRemoteMetadataReader() {
            return new BaseRemoteMetadataReader(this.connection, this.properties);
        }

        @Override
        protected QueryRewriter createQueryRewriter() {
            return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
        }
    }
}