package com.exasol.adapter.dialects.redshift;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

class RedshiftSqlGenerationVisitorTest {
    private RedshiftSqlGenerationVisitor visitor;

    @BeforeEach
    void beforeEach() {
        final SqlDialect dialect = new RedshiftSqlDialectFactory().createSqlDialect(null,
                AdapterProperties.emptyProperties());
        final SqlGenerationContext context = new SqlGenerationContext("test_catalog", "test_schema", false);
        this.visitor = new RedshiftSqlGenerationVisitor(dialect, context);
    }

    @Test
    void visitSqlFunctionAggregateGroupConcat() throws AdapterException {
        final SqlLiteralString argument = new SqlLiteralString("value");
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("\"test_column").type(DataType.createBool())
                .build();
        final ColumnMetadata columnMetadata2 = ColumnMetadata.builder().name("test_column2\"")
                .type(DataType.createDouble()).build();
        final List<SqlNode> orderByArguments = List.of(new SqlColumn(1, columnMetadata),
                new SqlColumn(2, columnMetadata2));
        final SqlOrderBy orderBy = new SqlOrderBy(orderByArguments, List.of(false, true), List.of(false, true));
        final SqlFunctionAggregateGroupConcat aggregateGroupConcat = SqlFunctionAggregateGroupConcat.builder(argument)
                .separator(new SqlLiteralString("|")).orderBy(orderBy).distinct(true).build();
        assertThat(this.visitor.visit(aggregateGroupConcat), equalTo(
                "LISTAGG('value', '|') WITHIN GROUP(ORDER BY \"\"\"test_column\" DESC NULLS FIRST, \"test_column2\"\"\")"));
    }
}