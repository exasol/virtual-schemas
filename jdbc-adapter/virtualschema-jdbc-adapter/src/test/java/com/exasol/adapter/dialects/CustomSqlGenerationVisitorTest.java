package com.exasol.adapter.dialects;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.MetadataException;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;
import com.exasol.utils.SqlTestUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;

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

    private SqlNode getTestSqlNode() throws MetadataException {
        TableMetadata clicksMeta = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", clicksMeta);
        SqlSelectList selectList = SqlSelectList.createRegularSelectList(
                ImmutableList.<SqlNode>of(new SqlPredicateNot(
                        new SqlPredicateNot(new SqlColumn(1, clicksMeta
                                .getColumns().get(0))))));
        return new SqlStatementSelect(fromClause, selectList, null, null, null,
                null, null);
    }

    private TableMetadata getTestTableMetadata() throws MetadataException {
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
        public String visit(SqlPredicateNot predicate) throws AdapterException {
            return "NOT_CUSTOM (" + predicate.getExpression().accept(this) + ")";
        }

    }

}
