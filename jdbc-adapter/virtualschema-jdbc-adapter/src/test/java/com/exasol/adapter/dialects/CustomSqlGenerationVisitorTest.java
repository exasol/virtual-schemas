package com.exasol.adapter.dialects;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;
import utils.SqlTestUtil;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CustomSqlGenerationVisitorTest {

    /**
     * This tests uses a SQL with nested expressions (NOT), to make sure that
     * the custom sql generation visitor is used for all levels of recursion.
     */
    @Test
    public void testSqlGenerator() throws AdapterException {
        final SqlNode node = getTestSqlNode();
        final String schemaName = "SCHEMA";
        final String expectedSql = "SELECT NOT_CUSTOM (NOT_CUSTOM (\"C1\")) FROM \"" + schemaName
                + "\".\"TEST\"";
        final SqlGenerationContext context = new SqlGenerationContext("", schemaName,
                false, false);
        final SqlDialectContext dialectContext = new SqlDialectContext(SchemaAdapterNotes.builder().build());
        final SqlGenerationVisitor generator = new TestSqlGenerationVisitor(
                new ExasolSqlDialect(dialectContext), context);
        final String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql),
                SqlTestUtil.normalizeSql(actualSql));
    }

    private SqlNode getTestSqlNode() throws MetadataException {
        final TableMetadata clicksMeta = getTestTableMetadata();
        final SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        final SqlSelectList selectList = SqlSelectList.createRegularSelectList(
                ImmutableList.<SqlNode>of(new SqlPredicateNot(
                        new SqlPredicateNot(new SqlColumn(1, clicksMeta
                                .getColumns().get(0))))));
        return new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, null);
    }

    private TableMetadata getTestTableMetadata() throws MetadataException {
        final List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("C1", "", DataType.createBool(), true,
                false, "", ""));
        return new TableMetadata("TEST", "", columns, "");
    }

    public static class TestSqlGenerationVisitor extends SqlGenerationVisitor {

        public TestSqlGenerationVisitor(final SqlDialect dialect,
                final SqlGenerationContext context) {
            super(dialect, context);
        }

        @Override
        public String visit(final SqlPredicateNot predicate) throws AdapterException {
            return "NOT_CUSTOM (" + predicate.getExpression().accept(this) + ")";
        }

    }

}
