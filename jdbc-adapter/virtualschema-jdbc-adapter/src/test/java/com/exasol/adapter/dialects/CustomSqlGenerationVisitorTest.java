package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.exasol.ExasolSqlDialect;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.sql.*;
import com.exasol.sql.SqlNormalizer;

class CustomSqlGenerationVisitorTest {
    /**
     * This tests uses a SQL with nested expressions (NOT), to make sure that the custom sql generation visitor is used
     * for all levels of recursion.
     *
     * @throws AdapterException if SQL node can't accept generator
     */
    @Test
    void testSqlGenerator() throws AdapterException {
        final SqlNode node = getTestSqlNode();
        final String schemaName = "SCHEMA";
        final String expectedSql = "SELECT NOT_CUSTOM (NOT_CUSTOM (\"C1\")) FROM \"" + schemaName + "\".\"TEST\"";
        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        final SqlNodeVisitor<String> generator = new TestSqlGenerationVisitor(
                new ExasolSqlDialect(null, AdapterProperties.emptyProperties()), context);
        final String actualSql = node.accept(generator);
        assertEquals(SqlNormalizer.normalizeSql(expectedSql), SqlNormalizer.normalizeSql(actualSql));
    }

    private SqlNode getTestSqlNode() {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(List.of(
                new SqlPredicateNot(new SqlPredicateNot(new SqlColumn(1, clicksMeta.getColumns().get(0))))));
        return new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);
    }

    private TableMetadata getTestTableMetadata() {
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(ColumnMetadata.builder().name("C1").adapterNotes("").type(DataType.createBool()).nullable(true)
                .identity(false).defaultValue("").comment("").build());
        return new TableMetadata("TEST", "", columns, "");
    }

    public static class TestSqlGenerationVisitor extends SqlGenerationVisitor {
        public TestSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
            super(dialect, context);
        }

        @Override
        public String visit(final SqlPredicateNot predicate) throws AdapterException {
            return "NOT_CUSTOM (" + predicate.getExpression().accept(this) + ")";
        }
    }
}