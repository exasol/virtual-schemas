package com.exasol.adapter.dialects;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.exasol.utils.SqlTestUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SqlDialectTest {

    @Test
    public void testAggregateFunctionAliases() throws AdapterException, MetadataException {
        TableMetadata clicksMeta = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        SqlColumn col1 = new SqlColumn(1, clicksMeta.getColumns().get(0));
        SqlSelectList selectList = SqlSelectList.createRegularSelectList( ImmutableList.<SqlNode>of(
                new SqlFunctionAggregate(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, ImmutableList.<SqlNode>of(col1), false),
                new SqlFunctionAggregate(AggregateFunction.AVG, ImmutableList.<SqlNode>of(col1), false),
                new SqlFunctionAggregate(AggregateFunction.COUNT, new ArrayList<SqlNode>(), true),
                new SqlFunctionAggregate(AggregateFunction.MAX, ImmutableList.<SqlNode>of(col1), false)
        ));
        SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        String schemaName = "SCHEMA";
        String expectedSql = "SELECT NDV(C1), AVERAGE(C1), COUNT2(DISTINCT *), MAX(C1) FROM " + schemaName + ".TEST";

        Map<AggregateFunction, String> aggAliases = new EnumMap<>(AggregateFunction.class);
        Map<ScalarFunction, String> scalarAliases = ImmutableMap.of();
        Map<ScalarFunction, String> infixAliases = ImmutableMap.of();
        aggAliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, "NDV");
        aggAliases.put(AggregateFunction.AVG, "AVERAGE");
        aggAliases.put(AggregateFunction.COUNT, "COUNT2");
        Map<ScalarFunction, String> prefixAliases = ImmutableMap.of();

        SqlDialect dialect = new AliasesSqlDialect(aggAliases, scalarAliases, infixAliases, prefixAliases);

        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    public void testScalarFunctionAliases() throws AdapterException, MetadataException {
        TableMetadata clicksMeta = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        SqlColumn col1 = new SqlColumn(1, clicksMeta.getColumns().get(0));
        SqlSelectList selectList = SqlSelectList.createRegularSelectList( ImmutableList.<SqlNode>of(
                new SqlFunctionScalar(ScalarFunction.ABS, ImmutableList.<SqlNode>of(col1), false, false),
                new SqlFunctionScalar(ScalarFunction.ADD, ImmutableList.of(col1, new SqlLiteralExactnumeric(new BigDecimal(100))), true, false),
                new SqlFunctionScalar(ScalarFunction.SUB, ImmutableList.of(col1, new SqlLiteralExactnumeric(new BigDecimal(100))), true, false),
                new SqlFunctionScalar(ScalarFunction.TO_CHAR, ImmutableList.<SqlNode>of(col1), true, false),
                new SqlFunctionScalar(ScalarFunction.NEG, ImmutableList.<SqlNode>of(col1), false, false)
        ));
        SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        String schemaName = "SCHEMA";
        // ADD is infix by default, but must be non-infix after applying the alias.
        String expectedSql = "SELECT ABSOLUTE(C1), PLUS(C1, 100), (C1 - 100), TO_CHAR(C1), NEGATIVE(C1) FROM " + schemaName + ".TEST";

        Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ABS, "ABSOLUTE");
        scalarAliases.put(ScalarFunction.ADD, "PLUS");
        scalarAliases.put(ScalarFunction.NEG, "NEGATIVE");
        SqlDialect dialect = new AliasesSqlDialect(ImmutableMap.<AggregateFunction, String>of(), scalarAliases, ImmutableMap.<ScalarFunction, String>of(), ImmutableMap.<ScalarFunction, String>of());

        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    public void testInvalidAliases() throws Exception {
        TableMetadata clicksMeta = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        SqlNode node = new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);

        SqlGenerationContext context = new SqlGenerationContext("", "schema", false);

        // Test non-simple scalar functions
        for (ScalarFunction function : ScalarFunction.values()) {
            if (!function.isSimple()) {
                Map<ScalarFunction, String> scalarAliases = ImmutableMap.of(function, "ALIAS");
                SqlDialect dialect = new AliasesSqlDialect(ImmutableMap.<AggregateFunction, String>of(), scalarAliases, ImmutableMap.<ScalarFunction, String>of(), ImmutableMap.<ScalarFunction, String>of());
                try {
                    SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
                    throw new Exception("Should never arrive here");
                } catch(RuntimeException ex) {
                    // This error is expected
                }
            }
        }

        // Test non-simple aggregate functions
        for (AggregateFunction function : AggregateFunction.values()) {
            if (!function.isSimple()) {
                Map<AggregateFunction, String> aggregateAliases = ImmutableMap.of(function, "ALIAS");
                SqlDialect dialect = new AliasesSqlDialect(aggregateAliases, ImmutableMap.<ScalarFunction, String>of(), ImmutableMap.<ScalarFunction, String>of(), ImmutableMap.<ScalarFunction, String>of());
                try {
                    SqlGenerationVisitor generator = new SqlGenerationVisitor(dialect, context);
                    throw new Exception("Should never arrive here");
                } catch(RuntimeException ex) {
                    // This error is expected
                }
            }
        }
    }

    private TableMetadata getTestTableMetadata() throws MetadataException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("C1", "", DataType.createBool(), true,
                false, "", ""));
        return new TableMetadata("TEST", "", columns, "");
    }

    static class AliasesSqlDialect extends AbstractSqlDialect {

        private Map<AggregateFunction, String> aggregationAliases;
        private Map<ScalarFunction, String> scalarAliases;
        private Map<ScalarFunction, String> infixAliases;
        private Map<ScalarFunction, String> prefixAliases;

        public AliasesSqlDialect(Map<AggregateFunction, String> aggregationAliases, Map<ScalarFunction, String> scalarAliases
                , Map<ScalarFunction, String> infixAliases, Map<ScalarFunction, String> prefixAliases) {
            super(new SqlDialectContext(new SchemaAdapterNotes(".", "\"", false, false, false, false, false, false, false, false, false, false, true, false)));
            this.aggregationAliases = aggregationAliases;
            this.scalarAliases = scalarAliases;
            this.infixAliases = infixAliases;
            this.prefixAliases = prefixAliases;
        }

        @Override
        public Capabilities getCapabilities() {
            Capabilities caps = new Capabilities();
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
            return aggregationAliases;
        }

        @Override
        public Map<ScalarFunction, String> getScalarFunctionAliases() {
            return scalarAliases;
        }

        @Override
        public Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
            if (infixAliases.isEmpty()) {
                return super.getBinaryInfixFunctionAliases();
            } else {
                return infixAliases;
            }
        }

        @Override
        public Map<ScalarFunction, String> getPrefixFunctionAliases() {
            if (prefixAliases.isEmpty()) {
                return super.getPrefixFunctionAliases();
            } else {
                return prefixAliases;
            }
        }

        @Override
        public String getPublicName() {
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
        public String applyQuote(String identifier) {
            return "\"" + identifier + "\"";
        }

        @Override
        public String applyQuoteIfNeeded(String identifier) {
            return identifier;  // Intentionally kept simple
        }

        @Override
        public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context) {
            return false;
        }

        @Override
        public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context) {
            return true;
        }

        @Override
        public NullSorting getDefaultNullSorting() {
            return NullSorting.NULLS_SORTED_HIGH;
        }

        @Override
        public String getStringLiteral(String value) {
            return "'" + value + "'";
        }
    }

}