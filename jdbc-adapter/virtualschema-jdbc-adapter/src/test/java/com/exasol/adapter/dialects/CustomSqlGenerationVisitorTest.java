package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlPredicateNot;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;
import com.exasol.utils.SqlTestUtil;
import org.mockito.Mockito;

public class CustomSqlGenerationVisitorTest {

    /**
     * This tests uses a SQL with nested expressions (NOT), to make sure that
     * the custom sql generation visitor is used for all levels of recursion.
     */
    @Test
    public void testSqlGenerator() {
        SqlNode node = getTestSqlNode();
        String schemaName = "SCHEMA";
        String expectedSql = "SELECT NOT_CUSTOM (NOT_CUSTOM (C1)) FROM " + schemaName
                + ".TEST";
        SqlGenerationContext context = new SqlGenerationContext("", schemaName,
                false);
        SqlDialectContext dialectContext = new SqlDialectContext(Mockito.mock(SchemaAdapterNotes.class));
        SqlGenerationVisitor generator = new TestSqlGenerationVisitor(
                new ExasolSqlDialect(dialectContext), context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql),
                SqlTestUtil.normalizeSql(actualSql));
    }

    private SqlNode getTestSqlNode() {
        TableMetadata clicksMeta = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        SqlSelectList selectList = new SqlSelectList(
                ImmutableList.<SqlNode>of(new SqlPredicateNot(
                        new SqlPredicateNot(new SqlColumn(1, clicksMeta
                                .getColumns().get(0))))));
        return new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, null);
    }

    private TableMetadata getTestTableMetadata() {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("C1", "", DataType.createBool(), true,
                false, "", ""));
        return new TableMetadata("TEST", "", columns, "");
    }

    public static class TestSqlGenerationVisitor extends SqlGenerationVisitor {

        public TestSqlGenerationVisitor(SqlDialect dialect,
                SqlGenerationContext context) {
            super(dialect, context);
        }

        @Override
        public String visit(SqlPredicateNot predicate) {
            return "NOT_CUSTOM (" + predicate.getExpression().accept(this) + ")";
        }

    }

}
